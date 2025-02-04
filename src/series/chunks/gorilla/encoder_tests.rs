#[cfg(test)]
mod tests {
    use std::time::Duration;
    use crate::series::chunks::gorilla::GorillaEncoder;
    use metricsql_runtime::types::{Sample, Timestamp, TimestampTrait};
    use crate::tests::generators::{generate_series_data, GeneratorOptions, RandAlgo};

    #[test]
    fn test_gorilla_encoder_encode_decode() {
        let tests = vec![
            (
                "one data point",
                vec![Sample::new(1600000000, 0.1)],
                false,
            ),
            (
                "data points at regular intervals",
                vec![
                    Sample::new(1600000000, 0.1),
                    Sample::new(1600000060, 0.1),
                    Sample::new(1600000120, 0.1),
                    Sample::new(1600000180, 0.1),
                ],
                false,
            ),
            (
                "data points at random intervals",
                vec![
                    Sample::new(1600000000, 0.1),
                    Sample::new(1600000060, 1.1),
                    Sample::new(1600000182, 15.01),
                    Sample::new(1600000400, 0.01),
                    Sample::new(1600002000, 10.8),
                ],
                false,
            ),
        ];

        for (name, input, want_err) in tests {
            println!("Running test: {}", name);
            let mut encoder = GorillaEncoder::new();
            for point in &input {
                encoder.add_sample(point).unwrap();
            }

            let buf = encoder.buf();

            let got = encoder.iter().collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(input, got);
        }
    }
    
    const ONE_DAY: Duration = Duration::from_secs(86400);
    
    #[test]
    fn test_gorilla_encoder_encode_decode_many() {
        let now = Timestamp::now();
        let start = now.sub(4 * ONE_DAY);
        let mut options = GeneratorOptions::new(start, now, 1000).unwrap();
        options.typ = RandAlgo::MackeyGlass;
        
        let data = generate_series_data(&options).unwrap();

        let mut encoder = GorillaEncoder::new();
        for sample in data.iter() {
            encoder.add_sample(sample).unwrap();
        }

        let buf = encoder.buf();

        let got = encoder.iter().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(data, got);
    }

}