use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

type BinaryData = Vec<u8>;
type KeyType = BinaryData;
type Expiry = Option<Instant>;
type ValueType = (BinaryData, Expiry);

pub struct ExpiringHashMap {
    store: Arc<Mutex<HashMap<KeyType, ValueType>>>,
    gc_stop_cv: Arc<Condvar>,
    gc_stop_requested: Arc<Mutex<bool>>,
    gc_handle: Option<JoinHandle<()>>,
}

impl Drop for ExpiringHashMap {
    fn drop(&mut self) {
        if let Some(handle) = self.gc_handle.take() {
            {
                let mut gc_stop_required = self.gc_stop_requested.lock().unwrap();
                if !*gc_stop_required {
                    *gc_stop_required = true;
                    self.gc_stop_cv.notify_one();
                }
            }
            _ = handle.join();
        }
    }
}

impl Default for ExpiringHashMap {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpiringHashMap {
    pub fn new() -> Self {
        let store = Arc::new(Mutex::new(HashMap::new()));
        let gc_stop_cv = Arc::new(Condvar::new());
        let gc_stop_requested = Arc::new(Mutex::new(false));
        ExpiringHashMap {
            store: store.clone(),
            gc_stop_cv: gc_stop_cv.clone(),
            gc_stop_requested: gc_stop_requested.clone(),
            gc_handle: Some(thread::spawn(move || {
                let sleep_timeout = Duration::from_secs(5); // 5mins
                loop {
                    {
                        let stop_guard = gc_stop_requested.lock().unwrap();
                        let stop_requested = gc_stop_cv
                            .wait_timeout(stop_guard, sleep_timeout)
                            .unwrap()
                            .0;
                        if *stop_requested {
                            println!("INFO: stopping GC thread");
                            break;
                        }
                    }

                    let num_expired_keys;
                    {
                        let mut store = store.lock().unwrap();
                        let current_time = Instant::now();
                        let mut expired_keys = vec![];
                        for (key, (_, ttl)) in store.iter() {
                            if let Some(ttl) = ttl {
                                if ttl < &current_time {
                                    expired_keys.push(key.clone());
                                }
                            }
                        }
                        num_expired_keys = expired_keys.len();
                        for key in expired_keys.into_iter() {
                            store.remove(&key);
                        }
                    }

                    if num_expired_keys > 0 {
                        println!("INFO: cleaned up {} expired keys", num_expired_keys);
                    }
                }
            })),
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let mut store = self.store.lock().unwrap();
        if let Some((_, Some(ttl))) = store.get(key) {
            if ttl < &Instant::now() {
                store.remove(key);
                return None;
            }
        }

        store.get(key).map(|(value, _)| value).cloned()
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], expiry: Option<Duration>) {
        let ttl = expiry.map(|duration| Instant::now() + duration);
        self.store
            .lock()
            .unwrap()
            .insert(key.to_vec(), (value.to_vec(), ttl));
    }
}
