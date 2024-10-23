#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use valkey_module::ValkeyError;
    use crate::common::current_time_millis;
    use crate::error_consts;
    use crate::series::timestamp_range::TimestampValue;
    use crate::series::{TimeSeries, TimestampRange};

    #[test]
    fn test_timestamp_range_value_try_from_earliest() {
        let input = "-";
        let result = TimestampValue::try_from(input);
        assert!(matches!(result, Ok(TimestampValue::Earliest)), "Expected Ok(Earliest), got {:?}", result);
    }

    #[test]
    fn test_timestamp_range_value_try_from_latest() {
        let input = "+";
        let result = TimestampValue::try_from(input);
        assert!(matches!(result, Ok(TimestampValue::Latest)), "Expected Ok(Latest), got {:?}", result);
    }

    #[test]
    fn test_timestamp_range_value_try_from_now() {
        let input = "*";
        let result = TimestampValue::try_from(input);
        assert!(matches!(result, Ok(TimestampValue::Now)), "Expected Ok(Now), got {:?}", result);
    }

    #[test]
    fn test_timestamp_range_value_try_from_positive_number() {
        let input = "12345678";
        let result = TimestampValue::try_from(input);
        let expected = 12345678;
        assert!(matches!(result, Ok(TimestampValue::Value(expected))), "Expected Ok(Value({})), got {:?}", expected, result);
    }

    fn test_timestamp_range_value_try_from_negative_number() {
        let input = "-12345678";
        let result = TimestampValue::try_from(input);
        let expected = -12345678;
        assert!(matches!(result, Ok(TimestampValue::Value(expected))), "Expected Ok(Value({})), got {:?}", expected, result);
    }


    #[test]
    fn test_timestamp_range_value_try_from_positive_duration() {
        let input = "+5s";
        let result = TimestampValue::try_from(input);
        let expected_delta = 5000;
        assert!(matches!(result, Ok(TimestampValue::Relative(expected_delta))), "Expected Ok(Relative({})), got {:?}", expected_delta, result);
    }

    #[test]
    fn test_timestamp_range_value_try_from_negative_duration() {
        let input = "-5s";
        let result = TimestampValue::try_from(input);
        let expected_delta = -5000;
        assert!(matches!(result, Ok(TimestampValue::Relative(expected_delta))), "Expected Ok(Relative(-{})), got {:?}", expected_delta, result);
    }

    #[test]
    fn test_timestamp_range_value_try_from_invalid_timestamp() {
        let input = "invalid_timestamp";
        let result = TimestampValue::try_from(input);
        assert!(matches!(result, Err(ValkeyError::Str(error_consts::INVALID_TIMESTAMP))));
    }

    #[test]
    fn test_timestamp_value_as_timestamp_now() {
        let now = TimestampValue::Now;
        let current_time = current_time_millis();
        let result = now.as_timestamp();
        assert!((current_time..=current_time + 1).contains(&result), "Expected current time, got {}", result);
    }

    #[test]
    fn test_timestamp_value_as_timestamp_value() {
        let specific_timestamp = 1627849200000; // Example specific timestamp
        let value = TimestampValue::Value(specific_timestamp);
        let result = value.as_timestamp();
        assert_eq!(result, specific_timestamp, "Expected {}, got {}", specific_timestamp, result);
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_earliest() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_timestamp: 2000,
            ..Default::default()
        };
        let earliest = TimestampValue::Earliest;
        let result = earliest.as_series_timestamp(&series);
        assert_eq!(result, series.first_timestamp, "Expected {}, got {}", series.first_timestamp, result);
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_latest() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_timestamp: 2000,
            ..Default::default()
        };
        let latest = TimestampValue::Latest;
        let result = latest.as_series_timestamp(&series);
        assert_eq!(result, series.last_timestamp, "Expected {}, got {}", series.last_timestamp, result);
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_relative_positive_delta() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_timestamp: 2000,
            ..Default::default()
        };
        let positive_delta = 5000; // 5 seconds
        let relative = TimestampValue::Relative(positive_delta);
        let current_time = current_time_millis();
        let result = relative.as_series_timestamp(&series);
        assert_eq!(result, current_time.wrapping_add(positive_delta), "Expected {}, got {}", current_time.wrapping_add(positive_delta), result);
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_relative_negative_delta() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_timestamp: 2000,
            ..Default::default()
        };
        let negative_delta = -5000; // -5 seconds
        let relative = TimestampValue::Relative(negative_delta);
        let current_time = current_time_millis();
        let result = relative.as_series_timestamp(&series);
        assert_eq!(result, current_time.wrapping_add(negative_delta), "Expected {}, got {}", current_time.wrapping_add(negative_delta), result);
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_relative_overflow() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_timestamp: 2000,
            ..Default::default()
        };
        let large_delta = i64::MAX; // A large positive delta that will cause overflow
        let relative = TimestampValue::Relative(large_delta);
        let current_time = current_time_millis();
        let result = relative.as_series_timestamp(&series);
        assert_eq!(result, current_time.wrapping_add(large_delta), "Expected {}, got {}", current_time.wrapping_add(large_delta), result);
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_relative_underflow() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_timestamp: 2000,
            ..Default::default()
        };
        let large_negative_delta = i64::MIN; // A large negative delta that will cause underflow
        let relative = TimestampValue::Relative(large_negative_delta);
        let current_time = current_time_millis();
        let result = relative.as_series_timestamp(&series);
        assert_eq!(result, current_time.wrapping_add(large_negative_delta), "Expected {}, got {}", current_time.wrapping_add(large_negative_delta), result);
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_value_zero() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_timestamp: 2000,
            ..Default::default()
        };
        let zero_value = TimestampValue::Value(0);
        let result = zero_value.as_series_timestamp(&series);
        assert_eq!(result, 0, "Expected 0, got {}", result);
    }

    #[test]
    fn test_timestamp_value_as_series_timestamp_now() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_timestamp: 2000,
            ..Default::default()
        };
        let now = TimestampValue::Now;
        let current_time = current_time_millis();
        let result = now.as_series_timestamp(&series);
        assert!((current_time..=current_time + 1).contains(&result), "Expected current time, got {}", result);
    }

    #[test]
    fn test_timestamp_partial_cmp_relative_values() {
        let delta = 5000; // 5 seconds
        let relative1 = TimestampValue::Relative(delta);
        let relative2 = TimestampValue::Relative(delta);
        let result = relative1.partial_cmp(&relative2);
        assert_eq!(result, Some(Ordering::Equal), "Expected Ordering::Equal, got {:?}", result);

        assert!(TimestampValue::Relative(1000) < TimestampValue::Relative(2000));
        assert!(TimestampValue::Relative(1000) <= TimestampValue::Relative(2000));

        assert!(TimestampValue::Relative(3400) > TimestampValue::Relative(2000));
        assert!(TimestampValue::Relative(3400) >= TimestampValue::Relative(2000));
    }

    #[test]
    fn test_partial_cmp_earliest_with_other_values() {
        use std::cmp::Ordering;
        use crate::series::timestamp_range::TimestampValue;

        let earliest = TimestampValue::Earliest;
        let latest = TimestampValue::Latest;
        let now = TimestampValue::Now;
        let value = TimestampValue::Value(1000);
        let relative = TimestampValue::Relative(5000);

        assert_eq!(earliest.partial_cmp(&latest), Some(Ordering::Less), "Expected Earliest < Latest");
        assert_eq!(earliest.partial_cmp(&now), Some(Ordering::Less), "Expected Earliest < Now");
        assert_eq!(earliest.partial_cmp(&value), Some(Ordering::Less), "Expected Earliest < Value");
        assert_eq!(earliest.partial_cmp(&relative), Some(Ordering::Less), "Expected Earliest < Relative");
    }

    #[test]
    fn test_partial_cmp_greater_than_earliest() {
        use std::cmp::Ordering;
        use crate::series::timestamp_range::TimestampValue;

        let earliest = TimestampValue::Earliest;

        let values = vec![
            TimestampValue::Latest,
            TimestampValue::Now,
            TimestampValue::Value(1000),
            TimestampValue::Relative(1000),
        ];

        for value in values {
            let result = value.partial_cmp(&earliest);
            assert_eq!(result, Some(Ordering::Greater), "Expected {:?} to be greater than Earliest, got {:?}", value, result);
        }
    }

    #[test]
    fn test_partial_cmp_latest_greater_than_others() {
        use std::cmp::Ordering;
        use crate::series::timestamp_range::TimestampValue;

        let latest = TimestampValue::Latest;

        let values = vec![
            TimestampValue::Earliest,
            TimestampValue::Now,
            TimestampValue::Value(1000),
            TimestampValue::Relative(5000),
        ];

        for value in values {
            assert_eq!(latest.partial_cmp(&value), Some(Ordering::Greater), "Expected Latest to be greater than {:?}.", value);
        }
    }

    #[test]
    fn test_partial_cmp_with_latest() {
        use std::cmp::Ordering;
        use crate::series::timestamp_range::TimestampValue;

        let latest = TimestampValue::Latest;

        let earliest = TimestampValue::Earliest;
        assert_eq!(earliest.partial_cmp(&latest), Some(Ordering::Less), "Expected Earliest to be less than Latest");

        let now = TimestampValue::Now;
        assert_eq!(now.partial_cmp(&latest), Some(Ordering::Less), "Expected Now to be less than Latest");

        let value = TimestampValue::Value(1000);
        assert_eq!(value.partial_cmp(&latest), Some(Ordering::Less), "Expected Value to be less than Latest");

        let relative = TimestampValue::Relative(5000);
        assert_eq!(relative.partial_cmp(&latest), Some(Ordering::Less), "Expected Relative to be less than Latest");
    }

    #[test]
    fn test_partial_cmp_equal_values() {
        let timestamp1 = TimestampValue::Value(1627849200000);
        let timestamp2 = TimestampValue::Value(1627849200000);
        let result = timestamp1.partial_cmp(&timestamp2);
        assert_eq!(result, Some(Ordering::Equal), "Expected Ordering::Equal, got {:?}", result);
    }

    #[test]
    fn test_partial_cmp_now_with_relative_negative() {
        let now = TimestampValue::Now;
        let negative_delta = -1000; // -1 second
        let relative_negative = TimestampValue::Relative(negative_delta);

        let result = now.partial_cmp(&relative_negative);

        assert_eq!(result, Some(Ordering::Greater), "Expected Now to be greater than Relative(-1000), got {:?}", result);
    }

    #[test]
    fn test_partial_cmp_relative_with_now_positive() {
        let relative_value = TimestampValue::Relative(1000); // 1 second in the future
        let now_value = TimestampValue::Now;

        let result = relative_value.partial_cmp(&now_value);
        assert_eq!(result, Some(Ordering::Greater), "Expected Relative to be greater than Now, got {:?}", result);
    }

    #[test]
    fn test_partial_cmp_now_with_value_current_time() {
        let now = TimestampValue::Now;
        let current_time = current_time_millis();
        let value = TimestampValue::Value(current_time);
        let result = now.partial_cmp(&value);
        assert_eq!(result, Some(Ordering::Equal), "Expected Ordering::Equal, got {:?}", result);
    }

    #[test]
    fn test_partial_cmp_value_future_with_now() {
        let future_timestamp = current_time_millis() + 10000; // 10 seconds in the future
        let value = TimestampValue::Value(future_timestamp);
        let now = TimestampValue::Now;

        let result = value.partial_cmp(&now);

        assert_eq!(result, Some(Ordering::Greater), "Expected Value to be greater than Now, got {:?}", result);
    }

    #[test]
    fn test_timestamp_range_new_start_greater_than_end() {
        let start = TimestampValue::Value(2000);
        let end = TimestampValue::Value(1000);
        let result = TimestampRange::new(start, end);

        assert!(matches!(result, Err(ValkeyError::Str(err)) if err == "ERR invalid timestamp range: start > end"), "Expected Err with message 'ERR invalid timestamp range: start > end', got {:?}", result);
    }

    #[test]
    fn test_timestamp_range_new_negative_start_greater_than_end() {
        let start = TimestampValue::Value(-1000);
        let end = TimestampValue::Value(-2000);
        let result = TimestampRange::new(start, end);

        assert!(matches!(result, Err(ValkeyError::Str(err)) if err == "ERR invalid timestamp range: start > end"), "Expected Err with message 'ERR invalid timestamp range: start > end', got {:?}", result);
    }

    #[test]
    fn test_timestamp_range_new_start_less_than_end() {
        let start = TimestampValue::Value(1000);
        let end = TimestampValue::Value(2000);
        let result = TimestampRange::new(start, end);

        assert!(matches!(result, Ok(range) if range.start == start && range.end == end),
                "Expected Ok with start and end values, got {:?}", result);
    }


    #[test]
    fn test_get_series_range_earliest_no_retention_check() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_timestamp: 2000,
            ..Default::default()
        };
        let timestamp_range = TimestampRange {
            start: TimestampValue::Earliest,
            end: TimestampValue::Latest,
        };
        let (start, end) = timestamp_range.get_series_range(&series, false);
        assert_eq!(start, series.first_timestamp, "Expected start to be the earliest timestamp, got {}", start);
        assert_eq!(end, series.last_timestamp, "Expected end to be the latest timestamp, got {}", end);
    }

    #[test]
    fn test_get_series_range_with_retention_limit() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_timestamp: 20000,
            retention: std::time::Duration::from_secs(5), // 5 seconds retention
            ..Default::default()
        };

        let start = TimestampValue::Earliest;
        let end = TimestampValue::Latest;
        let range = TimestampRange::new(start, end).unwrap();

        let (start_timestamp, end_timestamp) = range.get_series_range(&series, true);

        let expected_earliest_retention = series.last_timestamp - series.retention.as_millis() as i64;
        assert_eq!(start_timestamp, expected_earliest_retention, "Expected start timestamp to be adjusted to retention limit");
        assert_eq!(end_timestamp, series.last_timestamp, "Expected end timestamp to be the last timestamp of the series");
    }

    #[test]
    fn test_get_series_range_latest_end_no_retention_check() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_timestamp: 5000,
            ..Default::default()
        };
        let timestamp_range = TimestampRange {
            start: TimestampValue::Earliest,
            end: TimestampValue::Latest,
        };
        let check_retention = false;
        let (start, end) = timestamp_range.get_series_range(&series, check_retention);
        assert_eq!(start, series.first_timestamp, "Expected start timestamp to be {}, got {}", series.first_timestamp, start);
        assert_eq!(end, series.last_timestamp, "Expected end timestamp to be {}, got {}", series.last_timestamp, end);
    }

    #[test]
    fn test_get_series_range_with_relative_end() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_timestamp: 5000,
            ..Default::default()
        };
        let start = TimestampValue::Value(2000);
        let end_delta = 3000; // 3 seconds
        let end = TimestampValue::Relative(end_delta);
        let timestamp_range = TimestampRange::new(start, end).unwrap();

        let (start_timestamp, end_timestamp) = timestamp_range.get_series_range(&series, false);

        assert_eq!(start_timestamp, 2000, "Expected start timestamp to be 2000, got {}", start_timestamp);
        assert_eq!(end_timestamp, 5000, "Expected end timestamp to be 5000, got {}", end_timestamp);
    }

    #[test]
    fn test_get_series_range_now_start_no_retention() {
        let series = TimeSeries {
            first_timestamp: 1000,
            last_timestamp: 2000,
            ..Default::default()
        };
        let timestamp_range = TimestampRange {
            start: TimestampValue::Now,
            end: TimestampValue::Latest,
        };
        let current_time = current_time_millis();
        let (start_timestamp, end_timestamp) = timestamp_range.get_series_range(&series, false);
        assert!((current_time..=current_time + 1).contains(&start_timestamp), "Expected start timestamp to be current time, got {}", start_timestamp);
        assert_eq!(end_timestamp, series.last_timestamp, "Expected end timestamp to be {}, got {}", series.last_timestamp, end_timestamp);
    }

}