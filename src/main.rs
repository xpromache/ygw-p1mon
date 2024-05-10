use p1mon::P1Mon;
use ygw::{ygw_server::ServerBuilder, Result};

mod p1mon;


#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    //let node1 = P1Mon::new("/dev/ttyUSB0")?;
    let node1 = P1Mon::new("/dev/pts/7", "p1mon")?;
        
    let server = ServerBuilder::new()
    .add_node(Box::new(node1))
    .build();

    let handle = server.start().await?;

    if let Err(err) = handle.jh.await {
        println!("server terminated with error {:?}", err);
    }
   Ok(())
}

