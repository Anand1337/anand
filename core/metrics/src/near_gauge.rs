use crate::IntCounter;
use prometheus::core::{AtomicI64, AtomicU64, GenericCounter, GenericGauge};
use prometheus::{Histogram, HistogramOpts, HistogramTimer, IntGauge, Opts};

pub struct NearIntGauge {
    gauge: GenericGauge<AtomicI64>,
}

impl NearIntGauge {
    pub fn new(name: &'static str, help: &'static str) -> Self {
        let opts = Opts::new(name, help);
        let gauge = IntGauge::with_opts(opts).unwrap();
        prometheus::register(Box::new(gauge.clone())).unwrap();
        NearIntGauge { gauge }
    }

    pub fn inc(&'static self) {
        self.gauge.inc();
    }

    pub fn dec(&'static self) {
        self.gauge.dec();
    }

    pub fn set(&'static self, val: i64) {
        self.gauge.set(val);
    }
}

pub struct NearIntCounter {
    counter: GenericCounter<AtomicU64>,
}

impl NearIntCounter {
    pub fn new(name: &'static str, help: &'static str) -> Self {
        let opts = Opts::new(name, help);
        let counter = IntCounter::with_opts(opts).unwrap();
        prometheus::register(Box::new(counter.clone())).unwrap();

        NearIntCounter { counter }
    }

    pub fn inc(&'static self) {
        self.counter.inc();
    }

    pub fn inc_by(&'static self, val: u64) {
        self.counter.inc_by(val);
    }

    pub fn get(&'static self) -> u64 {
        self.counter.get()
    }
}
pub struct NearHistogram {
    histogram: Histogram,
}

impl NearHistogram {
    pub fn new(name: &'static str, help: &'static str) -> Self {
        let opts = HistogramOpts::new(name, help);
        let histogram = Histogram::with_opts(opts).unwrap();
        prometheus::register(Box::new(histogram.clone())).unwrap();
        NearHistogram { histogram }
    }

    pub fn start_timer(&'static self) -> HistogramTimer {
        self.histogram.start_timer()
    }
}
