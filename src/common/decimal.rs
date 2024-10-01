use std::f64::consts::LN_10;
use std::f64::consts::LN_2;

pub fn round_to_decimal_digits(f: f64, digits: i32) -> f64 {
    if digits <= -100 || digits >= 100 {
        return f;
    }
    let m = 10_f64.powi(digits);
    (f * m).round() / m
}

pub fn round_to_significant_figures(f: f64, digits: u32) -> f64 {
    if digits == 0 || digits >= 18 {
        return f;
    }

    if f.is_nan() || f.is_infinite() || f == 0.0 {
        return f;
    }

    let is_negative = f.is_sign_negative();
    let f = if is_negative { -f } else { f };
    let (mut v, mut e) = positive_float_to_decimal(f);

    let n = 10_i64.pow(digits);

    let mut rem = 0;
    while v > n {
        rem = v % 10;
        v /= 10;
        e += 1;
    }
    if rem >= 5 {
        v += 1;
    }
    if is_negative {
        v = -v;
    }
    to_float(v, e)
}

#[inline]
fn to_float(v: i64, e: i16) -> f64 {
    let f = v as f64;
    if e < 0 {
        f / 10_f64.powi(-e as i32)
    } else {
        f * 10_f64.powi(e as i32)
    }
}

fn positive_float_to_decimal(f: f64) -> (i64, i16) {
    let u = f as u64;
    if u as f64 != f {
        return positive_float_to_decimal_slow(f);
    }
    if u < 1 << 55 && u % 10 != 0 {
        return (u as i64, 0);
    }
    get_decimal_and_scale(u)
}

fn get_decimal_and_scale(mut u: u64) -> (i64, i16) {
    let mut scale = 0;
    while u >= 1 << 55 {
        u /= 10;
        scale += 1;
    }
    if u % 10 != 0 {
        return (u as i64, scale);
    }
    u /= 10;
    scale += 1;
    while u != 0 && u % 10 == 0 {
        u /= 10;
        scale += 1;
    }
    (u as i64, scale)
}

fn positive_float_to_decimal_slow(f: f64) -> (i64, i16) {
    let mut scale = 0;
    let mut prec = 1e12;
    let mut f = f;
    if !(1e-6..=1e6).contains(&f) {
        prec = if f > 1e6 {
            // Increase conversion precision for big numbers.
            // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/213
            1e15
        } else {
            prec
        };
        let (_mantissa, mut exp) = frexp(f);
        // Bound the exponent according to https://en.wikipedia.org/wiki/Double-precision_floating-point_format
        // This fixes the issue https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1114
        exp = exp.clamp(-1022, 1023);
        scale = (exp as f64 * (LN_2 / LN_10)) as i16;
        f *= 10_f64.powi(-scale as i32);
    }

    // Multiply f by 100 until the fractional part becomes too small comparing to integer part.
    while f < prec {
        let (x, frac) = modf(f);
        if frac * prec < x {
            f = x;
            break;
        }
        if (1.0 - frac) * prec < x {
            f = x + 1.0;
            break;
        }
        f *= 100.0;
        scale -= 2;
    }
    let mut u = f as u64;
    if u % 10 != 0 {
        return (u as i64, scale);
    }

    // Minimize u by converting trailing zero to scale.
    u /= 10;
    scale += 1;
    (u as i64, scale)
}


/// https://github.com/rust-lang/libm
pub fn frexp(x: f64) -> (f64, i32) {
    let mut y = x.to_bits();
    let ee = ((y >> 52) & 0x7ff) as i32;

    if ee == 0 {
        if x != 0.0 {
            let xlp64: f64 = f64::from_bits(0x43f0000000000000);
            let (x, e) = frexp(x * xlp64);
            return (x, e - 64);
        }
        return (x, 0);
    } else if ee == 0x7ff {
        return (x, 0);
    }

    let e = ee - 0x3fe;
    y &= 0x800fffffffffffff;
    y |= 0x3fe0000000000000;
    (f64::from_bits(y), e)
}

pub fn modf(x: f64) -> (f64, f64) {
    let rv2: f64;
    let mut u = x.to_bits();
    let e = ((u >> 52 & 0x7ff) as i32) - 0x3ff;

    /* no fractional part */
    if e >= 52 {
        rv2 = x;
        if e == 0x400 && (u << 12) != 0 {
            /* nan */
            return (x, rv2);
        }
        u &= 1 << 63;
        return (f64::from_bits(u), rv2);
    }

    /* no integral part*/
    if e < 0 {
        u &= 1 << 63;
        rv2 = f64::from_bits(u);
        return (x, rv2);
    }

    let mask = ((!0) >> 12) >> e;
    if (u & mask) == 0 {
        rv2 = x;
        u &= 1 << 63;
        return (f64::from_bits(u), rv2);
    }
    u &= !mask;
    rv2 = f64::from_bits(u);
    (x - rv2, rv2)
}


#[derive(Debug, PartialEq, Copy, Clone)]
pub enum RoundDirection {
    Up,
    Down,
    Nearest,
}

pub fn round_to_significant_digits_old(value: f64, digits: i32, dir: RoundDirection) -> f64 {
    if digits == 0 || digits >= 18 {
        return value;
    }

    if value.is_nan() || value.is_infinite() || value == 0.0 {
        return value;
    }

    let is_negative = value.is_sign_negative();

    let power = value.abs().log10().floor() - (digits - 1) as f64;
    let mult = 10.0_f64.powf(power);

    let intermediate = value / mult;

    match dir {
        RoundDirection::Up => intermediate.ceil(),
        RoundDirection::Down => intermediate.floor(),
        RoundDirection::Nearest => intermediate.round(),
    }
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

    let mut intermediate = f / mult;

    intermediate = intermediate.ceil();

    // let intermediate = match dir {
    //     RoundDirection::Up => intermediate.ceil(),
    //     RoundDirection::Down => intermediate.floor(),
    //     RoundDirection::Nearest => intermediate.round(),
    // };

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
        println!("   -123.456   rounded up   to 2 sig figures is {}", round_to_significant_digits_old(-123.456, 2, Up));
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