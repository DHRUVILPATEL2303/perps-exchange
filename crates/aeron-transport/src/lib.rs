use aeron_rs::aeron::Aeron;
use aeron_rs::concurrent::atomic_buffer::{AlignedBuffer, AtomicBuffer};
use aeron_rs::concurrent::logbuffer::header::Header;
use aeron_rs::context::Context;
use aeron_rs::publication::Publication;
use aeron_rs::subscription::Subscription;
use aeron_rs::utils::errors::AeronError;
use anyhow::{Result, anyhow};
use std::ffi::CString;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub struct AeronContext {
    pub aeron: Arc<Mutex<Aeron>>,
}

impl AeronContext {
    pub fn new(aeron_dir: &str) -> Result<Self> {
        let mut context = Context::new();
        context.set_aeron_dir(aeron_dir.to_string());

        let aeron =
            Aeron::new(context).map_err(|e| anyhow!("Failed to initialize Aeron: {:?}", e))?;

        Ok(Self {
            aeron: Arc::new(Mutex::new(aeron)),
        })
    }
}

pub struct AeronPublisher {
    publication: Arc<Mutex<Publication>>,
}

impl AeronPublisher {
    pub fn new(aeron: &Arc<Mutex<Aeron>>, channel: &str, stream_id: i32) -> Result<Self> {
        let c_channel = CString::new(channel).map_err(|_| anyhow!("Invalid channel string"))?;

        let registration_id = aeron
            .lock()
            .unwrap()
            .add_publication(c_channel, stream_id)
            .map_err(|e| anyhow!("Failed to add publication: {:?}", e))?;

        loop {
            match aeron.lock().unwrap().find_publication(registration_id) {
                Ok(publication) => return Ok(Self { publication }),
                Err(AeronError::PublicationNotReady(_)) => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(e) => {
                    return Err(anyhow!("Failed to find publication: {:?}", e));
                }
            }
        }
    }

    pub async fn publish(&self, message: &[u8]) -> Result<()> {
        let publication = self.publication.clone();

        loop {
            let res = {
                let aligned_buffer = AlignedBuffer::with_capacity(message.len() as i32);
                let atomic_buffer = AtomicBuffer::from_aligned(&aligned_buffer);
                atomic_buffer.put_bytes(0, message);
                
                let mut pub_lock = publication.lock().unwrap();
                pub_lock.offer(atomic_buffer)
            };

            match res {
                Ok(_pos) => return Ok(()),
                Err(AeronError::BackPressured) => {
                    tokio::time::sleep(Duration::from_micros(50)).await;
                }
                Err(e) => {
                    return Err(anyhow!("Aeron publication offer failed: {:?}", e));
                }
            }
        }
    }
}

pub struct AeronSubscriber {
    subscription: Arc<Mutex<Subscription>>,
}

impl AeronSubscriber {
    pub fn new(aeron: &Arc<Mutex<Aeron>>, channel: &str, stream_id: i32) -> Result<Self> {
        let c_channel = CString::new(channel).map_err(|_| anyhow!("Invalid channel string"))?;

        let registration_id = aeron
            .lock()
            .unwrap()
            .add_subscription(c_channel, stream_id)
            .map_err(|e| anyhow!("Failed to add subscription: {:?}", e))?;

        loop {
            match aeron.lock().unwrap().find_subscription(registration_id) {
                Ok(subscription) => return Ok(Self { subscription }),
                Err(AeronError::SubscriptionNotReady(_)) => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(e) => {
                    return Err(anyhow!("Failed to find subscription: {:?}", e));
                }
            }
        }
    }

    pub fn start_polling<F>(&self, mut handler: F) -> tokio::task::JoinHandle<()>
    where
        F: FnMut(&[u8]) + Send + 'static,
    {
        let subscription = self.subscription.clone();

        tokio::spawn(async move {
            let mut fragment_handler =
                |buffer: &AtomicBuffer, offset: i32, length: i32, _header: &Header| {
                    let data = buffer.as_sub_slice(offset, length);
                    handler(data);
                };

            loop {
                let fragments_read = {
                    let mut sub_lock = subscription.lock().unwrap();
                    sub_lock.poll(&mut fragment_handler, 10)
                };

                if fragments_read == 0 {
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
            }
        })
    }
}

unsafe impl Send for AeronContext {}
unsafe impl Sync for AeronContext {}

unsafe impl Send for AeronPublisher {}
unsafe impl Sync for AeronPublisher {}

unsafe impl Send for AeronSubscriber {}
unsafe impl Sync for AeronSubscriber {}
