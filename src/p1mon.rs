use std::io::{self, BufRead, BufReader};
use std::str;
use std::time::Duration;
use std::{collections::HashMap, fs::File};

use async_trait::async_trait;
use chrono::{Datelike, NaiveDateTime, Timelike};
use serialport::SerialPort;
use tokio::sync::mpsc::{Receiver, Sender};
use ygw::protobuf::ygw::{ParameterData, ParameterDefinitionList};
use ygw::utc_converter::{utc_to_instant, DateTimeComponents};
use ygw::{
    msg::{Addr, YgwMessage},
    protobuf::ygw::{ParameterDefinition, ParameterValue, Timestamp, Value},
    Link, LinkStatus, Result, YgwError, YgwLinkNodeProperties, YgwNode,
};

enum ParserState {
    LookForStart,
    LookForEnd,
}

#[derive(Debug)]
enum DmsrParamType {
    Float,
    Integer,
    String,
}

impl DmsrParamType {
    fn from_str(s: &str) -> Result<DmsrParamType> {
        match s.to_lowercase().as_str() {
            "float" => Ok(DmsrParamType::Float),
            "integer" => Ok(DmsrParamType::Integer),
            "string" => Ok(DmsrParamType::String),
            _ => Err(YgwError::ParseError(format!(
                "cannot parse {} into a type",
                s
            ))),
        }
    }
}

#[derive(Debug)]
struct DmsrParam {
    description: String,
    // if the name is 'timestamp' the parameter will be parsed as time and used as gentime
    // if the name is 'ignore' the parameter will not be sent to Yamcs
    name: String,
    ptype: DmsrParamType,
    // set to true when the parameter has been received and its value sent to Yamcs
    defined: bool,
    pid: u32,
}

struct P1MonState {
    seq_count: u32,
    addr: Addr,
    tx: Sender<YgwMessage>,
    rx: Receiver<YgwMessage>,
}
pub struct P1Mon {
    props: YgwLinkNodeProperties,
    parameter_group: String,
    serial_port: Box<dyn SerialPort>,
    obis_codes: HashMap<String, DmsrParam>,
}

#[async_trait]
impl YgwNode for P1Mon {
    fn properties(&self) -> &YgwLinkNodeProperties {
        &self.props
    }

    fn sub_links(&self) -> &[Link] {
        &[]
    }

    async fn run(
        mut self: Box<Self>,
        node_id: u32,
        tx: Sender<YgwMessage>,
        rx: Receiver<YgwMessage>,
    ) -> Result<()> {
        let addr = Addr::new(node_id, 0);
        let mut link_status = LinkStatus::new(addr);
        let mut state = P1MonState {
            seq_count: 0,
            addr,
            tx,
            rx,
        };

        loop {
            //send an initial link status indicating that the link is up
            link_status.send(&state.tx).await?;
            if let Err(e) = self.process_serial_data(&mut state).await {
                link_status.state_failed(format!("{:?}", e));
            }
            if state.rx.is_closed() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
        Ok(())
    }
}

impl P1Mon {
    pub fn new(serial_device: &str, parameter_group: &str) -> Result<Self> {
        let obis_codes = read_codes()?;
        let serial_port = serialport::new(serial_device, 115_200)
            .timeout(std::time::Duration::from_millis(100))
            .open()
            .map_err(|e| {
                YgwError::DeviceAccessError(format!("Cannot access {serial_device}: {}", e))
            })?;

        Ok(Self {
            props: YgwLinkNodeProperties {
                name: "P1MON".to_owned(),
                description: "Monitor electricity usage via P1 port".to_owned(),
                tm: false,
                tc: false,
            },
            serial_port,
            obis_codes,
            parameter_group: parameter_group.to_owned(),
        })
    }
    /// read data from serial port
    /// returns only if there was an error
    async fn process_serial_data(&mut self, p1mon_state: &mut P1MonState) -> Result<()> {
        let ser = self.serial_port.try_clone().map_err(|e| YgwError::Other(Box::new(e)))?;
        let mut ser = BufReader::new(ser);

        let mut p1t = String::new();

        let mut state = ParserState::LookForStart;
        let mut m_idx = 0;

        while !p1mon_state.rx.is_closed() {
            let n_idx = p1t.len();

            match ser.read_line(&mut p1t) {
                Ok(0) => {
                    return Err(YgwError::IOError("While reading from serial port".into()
                    , io::Error::from(
                        io::ErrorKind::UnexpectedEof,
                    )));
                }
                Err(e) => {
                    log::warn!("Error reading from serial port: {}", e);
                    p1t.clear();
                    state = ParserState::LookForStart;
                    continue;
                }
                _ => {}
            }

            match state {
                ParserState::LookForStart => {
                    if p1t.as_bytes()[0] == b'/' {
                        state = ParserState::LookForEnd;
                        m_idx = p1t.len();
                    } else {
                        p1t.clear();
                    }
                }

                ParserState::LookForEnd => {
                    if p1t.as_bytes()[n_idx] == b'!' {
                        let Some(hex) = p1t.get(n_idx + 1..n_idx + 5) else {
                            log::warn!("Invalid line {}", &p1t[n_idx..]);
                            p1t.clear();
                            state = ParserState::LookForStart;
                            continue;
                        };
                        let Ok(crc) = u16::from_str_radix(hex, 16) else {
                            log::warn!("Cannot parse hex crc {hex}");
                            continue;
                        };
                        let computed_crc =
                            crc16::State::<crc16::ARC>::calculate(&p1t.as_bytes()[0..n_idx + 1]);
                        if crc != computed_crc {
                            log::info!("CRC verification failed")
                        } else {
                            self.process_p1telegram(p1mon_state, &p1t[m_idx..n_idx])
                                .await;
                        }
                        p1t.clear();
                        state = ParserState::LookForStart;
                    }
                }
            }
        }

        Ok(())
    }

    /// processes the telegram string into parameter values
    /// returns parameter values as well as parameter definitions for those parameters for which no definition was generated previously
    /// once the definition has been generated, the DmsrParam.defined is set to true
    async fn process_p1telegram(&mut self, p1mon_state: &mut P1MonState, p1t: &str) {
        let mut pdefs = Vec::new();
        let mut pvalues = Vec::new();
        let mut gentime = None;
        let now = ygw::protobuf::now();

        log::debug!("Processing telegram {p1t}");

        for line in p1t.lines() {
            if line.is_empty() {
                continue;
            }
            let Ok(v) = split_p1_line(line) else {
                log::warn!("Cannot parse p1 line {}", line);
                continue;
            };

            if let Some(dmsr_param) = self.obis_codes.get_mut(v[0]) {
                if dmsr_param.name == "ignore" {
                    continue;
                }

                let a: Vec<&str> = v[1].split("*").collect();
                let unit: Option<&str> = a.get(1).map(|&x| x);

                if !dmsr_param.defined {
                    pdefs.push(get_pdef(dmsr_param, unit));
                    dmsr_param.defined = true;
                }
                if dmsr_param.name == "timestamp" {
                    gentime = get_timestamp(a[0]);
                    if gentime.is_none() {
                        log::warn!("Cannot parse timestamp {}", a[0]);
                    }
                } else {
                    if let Some(pvalue) = get_pvalue(dmsr_param, a[0]) {
                        pvalues.push(pvalue);
                    }
                }
            } else {
                log::info!("no parameter for code {}", v[0]);
            }
        }

        if pdefs.len() > 0 {
            log::debug!("Sending definitions {:?}", pdefs);
            let pdef_list = ParameterDefinitionList { definitions: pdefs };
            let _ = p1mon_state
                .tx
                .send(YgwMessage::ParameterDefinitions(
                    p1mon_state.addr,
                    pdef_list,
                ))
                .await;
        }
        

        let generation_time = gentime.or(Some(now.clone()));

        if pvalues.len() > 0 {
            let pdata = ParameterData {
                parameters: pvalues,
                group: self.parameter_group.clone(),
                seq_num: p1mon_state.seq_count,
                generation_time,
                acquisition_time: Some(now)
            };

            p1mon_state.seq_count += 1;
            log::debug!("Sending parameter values {:?}", pdata);
            let _ = p1mon_state
                .tx
                .send(YgwMessage::ParameterData(p1mon_state.addr, pdata))
                .await;
        }
    }
}

fn get_pdef(dmsr_param: &DmsrParam, unit: Option<&str>) -> ParameterDefinition {
    ParameterDefinition {
        relative_name: dmsr_param.name.clone(),
        description: Some(dmsr_param.description.clone()),
        unit: unit.map(|s| s.to_owned()),
        ptype: format!("{:?}", dmsr_param.ptype),
        writable: Some(false),
        id: dmsr_param.pid,
    }
}

fn get_timestamp(str_value: &str) -> Option<Timestamp> {
    //skip the 'S' at the end
    let s = if str_value.ends_with('S') {
        &str_value[0..str_value.len() - 1]
    } else {
        str_value
    };

    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%y%m%d%H%M%S") {
        let t = utc_to_instant(DateTimeComponents {
            year: dt.year(),
            month: dt.month() as i32,
            day: dt.day() as i32,
            hour: dt.hour() as i32,
            minute: dt.minute() as i32,
            second: dt.second() as i32,
            millis: 0,
        })
        .into();
        Some(t)
    } else {
        println!("bum");
        None
    }
}

fn get_pvalue(dmsr_param: &DmsrParam, str_value: &str) -> Option<ParameterValue> {
    let eng_value = match dmsr_param.ptype {
        DmsrParamType::Float => str_value.parse().ok().map(|x| Value {
            v: Some(ygw::protobuf::ygw::value::V::FloatValue(x)),
        }),
        DmsrParamType::Integer => str_value.parse().ok().map(|x| Value {
            v: Some(ygw::protobuf::ygw::value::V::Sint64Value(x)),
        }),
        DmsrParamType::String => Some(Value {
            v: Some(ygw::protobuf::ygw::value::V::StringValue(
                str_value.to_owned(),
            )),
        }),
    };

    let pv = ParameterValue {
        id: dmsr_param.pid,
        raw_value: None,
        eng_value,
        acquisition_time: None,
        generation_time: None,
        expire_millis: None,
    };
    Some(pv)
}

//split a line of the form
// 'ABC(g1)(g2)(g3)'
// into ['ABC', 'g1', 'g2']
// it ignores stuff that might be in between
pub fn split_p1_line(p1line: &str) -> ygw::Result<Vec<&str>> {
    let mut result = Vec::new();
    // 0 = before
    // 1 = inside
    // 2 = outside
    let mut state = 0;
    let mut k = 0;
    for (i, c) in p1line.chars().enumerate() {
        match c {
            '(' => {
                if state == 0 {
                    result.push(&p1line[..i]);
                } else if state != 2 {
                    return Err(ygw::YgwError::DecodeError(format!(
                        "Invalid string '{p1line}'"
                    )));
                }
                state = 1;
                k = i;
            }
            ')' => {
                if state != 1 {
                    return Err(ygw::YgwError::DecodeError(format!(
                        "Invalid string '{p1line}'"
                    )));
                }
                result.push(&p1line[k + 1..i]);
                state = 2;
            }
            _ => {}
        }
    }

    if state != 2 {
        return Err(ygw::YgwError::DecodeError(format!(
            "Invalid string '{p1line}'"
        )));
    }

    Ok(result)
}

fn read_codes() -> Result<HashMap<String, DmsrParam>> {
    let file = File::open("obiscodes.csv")?;

    let reader = io::BufReader::new(file);

    let mut m = HashMap::new();
    let mut pid = 0;

    for line in reader.lines() {
        if let Ok(line) = line {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = line.split(',').collect();
            if parts.len() == 4 {
                m.insert(
                    parts[0].to_owned(),
                    DmsrParam {
                        name: parts[1].to_owned(),
                        ptype: DmsrParamType::from_str(parts[2])?,
                        description: parts[3].to_owned(),
                        defined: false,
                        pid,
                    },
                );
                pid += 1;
            } else {
                return Err(YgwError::DecodeError(format!(
                    "wrong OBIS code definition '{line}'"
                )));
            }
        }
    }

    Ok(m)
}

#[cfg(test)]
mod tests {
    use ygw::utc_converter::{self, Instant};

    use super::*;

    #[test]
    fn test_extract_groups() {
        let input = "1-0:32.7.0(235.2*V)(40*A)(Test*T)";
        let expected = vec!["1-0:32.7.0", "235.2*V", "40*A", "Test*T"];
        let result = split_p1_line(input).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_extract_groups_err() {
        let input = "1-0:32.7.0";
        let result = split_p1_line(input);
        assert!(result.is_err())
    }
    #[test]
    fn test_timestamp() {
        let t = get_timestamp("240506201011S").unwrap();
        let t  = Instant::from(t);

        assert_eq!(utc_converter::to_string(t), "2024-05-06T20:10:11.000Z");
    }
}
