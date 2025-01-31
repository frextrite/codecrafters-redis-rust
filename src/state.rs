use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex, RwLock},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

type BinaryData = Vec<u8>;
type KeyType = BinaryData;
type Expiry = Option<Instant>;
type ValueType = (BinaryData, Expiry);
type Store = RwLock<HashMap<KeyType, ValueType>>;
type StopCondition = (Mutex<bool>, Condvar);

const SLEEP_TIMEOUT: Duration = Duration::from_secs(60);

pub struct ExpiringHashMap {
    store: Arc<Store>,
    gc_stop: Arc<StopCondition>,
    gc_handle: Option<JoinHandle<()>>,
}

impl Drop for ExpiringHashMap {
    fn drop(&mut self) {
        if let Some(handle) = self.gc_handle.take() {
            let (gc_stop_requested, gc_stop_cv) = &*self.gc_stop;
            *gc_stop_requested.lock().unwrap() = true;
            gc_stop_cv.notify_all();
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
        let mut map = ExpiringHashMap {
            store: Arc::new(RwLock::new(HashMap::new())),
            gc_stop: Arc::new((Mutex::new(false), Condvar::new())),
            gc_handle: None,
        };

        map.start_gc_thread();

        map
    }

    fn start_gc_thread(&mut self) {
        let store = self.store.clone();
        let gc_stop = self.gc_stop.clone();

        self.gc_handle = Some(thread::spawn(move || {
            Self::gc_thread_function(store, gc_stop);
        }));
    }

    fn gc_thread_function(store: Arc<Store>, gc_stop: Arc<StopCondition>) {
        loop {
            let (gc_stop_requested, _) = &*gc_stop;
            let stop_requested = *gc_stop_requested.lock().unwrap();
            if stop_requested {
                println!("INFO: stopping GC thread");
                break;
            }

            let num_expired_keys = Self::cleanup_expired_keys(store.clone());

            if num_expired_keys > 0 {
                println!("INFO: cleaned up {} expired keys", num_expired_keys);
            }

            let (gc_stop_requested, gc_stop_cv) = &*gc_stop;
            let _ = gc_stop_cv.wait_timeout(gc_stop_requested.lock().unwrap(), SLEEP_TIMEOUT);
        }
    }

    fn cleanup_expired_keys(store: Arc<Store>) -> usize {
        let mut store = store.write().unwrap();

        let current_time = Instant::now();
        let mut num_expired_keys = 0;

        store.retain(|_, (_, expiry)| {
            if let Some(expiry) = expiry.as_ref() {
                if expiry < &current_time {
                    num_expired_keys += 1;
                    return false;
                }
            }

            true
        });

        num_expired_keys
    }

    pub fn get(&self, key: &[u8]) -> Option<BinaryData> {
        let store = self.store.write().unwrap();

        if let Some((_, Some(ttl))) = store.get(key) {
            if ttl < &Instant::now() {
                drop(store);
                self.store.write().unwrap().remove(key);
                return None;
            }
        }

        store.get(key).map(|(value, _)| value).cloned()
    }

    pub fn set(&self, key: &[u8], value: &[u8], expiry: Option<Duration>) {
        let ttl = Self::calculate_ttl(expiry);
        let mut store = self.store.write().unwrap();

        store.insert(key.to_vec(), (value.to_vec(), ttl));
    }

    fn calculate_ttl(expiry: Option<Duration>) -> Option<Instant> {
        expiry.and_then(|duration| Instant::now().checked_add(duration))
    }
}
