#[cfg(feature = "metrics")]
mod tests {
    use metrics::{Counter, CounterFn, Key, KeyName, Metadata, Recorder, SharedString, Unit};
    use renoir::operator::source::IteratorSource;
    use renoir::prelude::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct MockRecorder {
        counters: Arc<Mutex<HashMap<String, u64>>>,
    }

    struct MockCounter {
        name: String,
        counters: Arc<Mutex<HashMap<String, u64>>>,
    }

    impl CounterFn for MockCounter {
        fn increment(&self, value: u64) {
            let mut counters = self.counters.lock().unwrap();
            *counters.entry(self.name.clone()).or_default() += value;
        }

        fn absolute(&self, _value: u64) {}
    }

    impl Recorder for MockRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        }
        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
        fn describe_histogram(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            let mut full_name = key.name().to_string();
            // Sort labels to ensure stable representation in the test
            let mut labels = key
                .labels()
                .map(|l| format!("{}={}", l.key(), l.value()))
                .collect::<Vec<_>>();
            labels.sort();
            if !labels.is_empty() {
                full_name.push_str(";");
                full_name.push_str(&labels.join(";"));
            }
            Counter::from_arc(Arc::new(MockCounter {
                name: full_name,
                counters: self.counters.clone(),
            }))
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    #[test]
    fn test_metrics_recorded() {
        let recorder = MockRecorder::default();
        let counters = recorder.counters.clone();

        // Register the global recorder
        metrics::set_global_recorder(recorder).unwrap();

        let config = RuntimeConfig::local(1).unwrap();
        let env = StreamContext::new(config);

        let source = IteratorSource::new(0..10u8);
        let res = env.stream(source).map(|n| n * 2).collect_vec();

        env.execute_blocking();

        let output = res.get().expect("No result returned");
        assert_eq!(output, (0..10u8).map(|n| n * 2).collect::<Vec<u8>>());

        // Check if metrics were logged in the mock recorder
        let counters_guard = counters.lock().unwrap();

        // Assert that we have recorded items_in and items_out metrics
        // (At least some counters starting with renoir_items_in or renoir_items_out should be present)
        let has_items_in = counters_guard
            .keys()
            .any(|k| k.starts_with("renoir_items_in"));
        let has_items_out = counters_guard
            .keys()
            .any(|k| k.starts_with("renoir_items_out"));

        println!("Recorded counters: {:?}", *counters_guard);
        assert!(has_items_in, "Should have logged renoir_items_in metric");
        assert!(has_items_out, "Should have logged renoir_items_out metric");
    }

    #[test]
    fn test_custom_resolution_env() {
        // Set the custom resolution to a very high value (e.g. 50000 ms) before anything gets initialized
        std::env::set_var("RENOIR_METRICS_RESOLUTION_MS", "50000");

        let recorder = MockRecorder::default();
        let counters = recorder.counters.clone();

        metrics::set_global_recorder(recorder).unwrap();

        let config = RuntimeConfig::local(1).unwrap();
        let env = StreamContext::new(config);

        let source = IteratorSource::new(0..10u8);
        let res = env.stream(source).map(|n| n * 2).collect_vec();

        env.execute_blocking();

        let output = res.get().expect("No result returned");
        assert_eq!(output, (0..10u8).map(|n| n * 2).collect::<Vec<u8>>());

        // Check if metrics were logged. Since resolution is 50s, the metrics
        // will not be flushed during execution (which takes milliseconds),
        // but they MUST be flushed when the MetricsProfiler is dropped at thread exit.
        let counters_guard = counters.lock().unwrap();
        let has_items_in = counters_guard
            .keys()
            .any(|k| k.starts_with("renoir_items_in"));
        let has_items_out = counters_guard
            .keys()
            .any(|k| k.starts_with("renoir_items_out"));

        println!(
            "Recorded counters with 50s resolution: {:?}",
            *counters_guard
        );
        assert!(
            has_items_in,
            "Should have logged renoir_items_in metric on drop"
        );
        assert!(
            has_items_out,
            "Should have logged renoir_items_out metric on drop"
        );
    }
}
