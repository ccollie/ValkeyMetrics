
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum RoundDirection {
    Up,
    Down,
    Nearest,
}

pub fn round_to_significant_digits(value: f64, digits: i32, dir: RoundDirection) -> f64 {
    if digits == 0 || digits >= 18 {
        return value;
    }

    if value.is_nan() || value.is_infinite() || value == 0.0 {
        return value;
    }

    let is_negative = value.is_sign_negative();
    let f = if is_negative { -value } else { value };

    let power = f.abs().log10().floor() - (digits - 1) as f64;
    let mult = 10.0_f64.powi(power as i32);

    let intermediate = f / mult;

    let intermediate = match dir {
        RoundDirection::Up => intermediate.ceil(),
        RoundDirection::Down => intermediate.floor(),
        RoundDirection::Nearest => intermediate.round(),
    };

    let result = intermediate * mult;
    if is_negative {
        return -result;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sig_dig_rounder() {
        use RoundDirection::*;
        println!("   -123.456   rounded up   to 2 sig figures is {}", round_to_significant_digits(-123.456, 2, Up));
        println!("     -0.03394 rounded down to 3 sig figures is {}", round_to_significant_digits(-0.03394, 3, Down));
        println!("    474       rounded up   to 2 sig figures is {}", round_to_significant_digits(474.0, 2, Up));
        println!("3004001       rounded down to 4 sig figures is {}", round_to_significant_digits(3004001.0, 4, Down));
    }

    #[test]
    fn test_round_to_significant_digits_up() {
        assert_eq!(round_to_significant_digits(-0.56, 1, RoundDirection::Up), -0.6);
        assert_eq!(round_to_significant_digits(123.456, 2, RoundDirection::Up), 130.0);
        assert_eq!(round_to_significant_digits(0.03394, 3, RoundDirection::Up), 0.0340);
        assert_eq!(round_to_significant_digits(474.0, 2, RoundDirection::Up), 480.0);
        assert_eq!(round_to_significant_digits(3004001.0, 4, RoundDirection::Up), 3005000.0);
        assert_eq!(round_to_significant_digits(-0.56, 1, RoundDirection::Up), -0.6);
    }

    #[test]
    fn test_round_to_significant_digits_down() {
        assert_eq!(round_to_significant_digits(123.456, 2, RoundDirection::Down), 120.0);
        assert_eq!(round_to_significant_digits(0.03394, 3, RoundDirection::Down), 0.0339);
        assert_eq!(round_to_significant_digits(474.0, 2, RoundDirection::Down), 470.0);
        assert_eq!(round_to_significant_digits(3004001.0, 4, RoundDirection::Down), 3004000.0);
    }

    #[test]
    fn test_round_to_significant_digits_nearest() {
        assert_eq!(round_to_significant_digits(123.456, 2, RoundDirection::Nearest), 120.0);
        assert_eq!(round_to_significant_digits(0.03394, 3, RoundDirection::Nearest), 0.0339);
        assert_eq!(round_to_significant_digits(474.0, 2, RoundDirection::Nearest), 470.0);
        assert_eq!(round_to_significant_digits(3004001.0, 4, RoundDirection::Nearest), 3004000.0);
    }

    #[test]
    fn test_round_to_significant_digits_edge_cases() {
        let nan_result = round_to_significant_digits(f64::NAN, 3, RoundDirection::Nearest);
        assert!(nan_result.is_nan());
        assert_eq!(round_to_significant_digits(f64::INFINITY, 3, RoundDirection::Nearest), f64::INFINITY);
        assert_eq!(round_to_significant_digits(0.0, 3, RoundDirection::Nearest), 0.0);
        assert_eq!(round_to_significant_digits(-123.456, 2, RoundDirection::Nearest), -120.0);
    }
}