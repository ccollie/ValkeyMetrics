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

const STALE_NAN_BITS: u64 = 0x7ff0000000000002;

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
    if f > 1e6 || f < 1e-6 {
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
    let mask: u64;
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

    mask = ((!0) >> 12) >> e;
    if (u & mask) == 0 {
        rv2 = x;
        u &= 1 << 63;
        return (f64::from_bits(u), rv2);
    }
    u &= !mask;
    rv2 = f64::from_bits(u);
    (x - rv2, rv2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_positive_float_to_decimal() {
        let test_case = |f: f64, decimal_expected: i64, exponent_expected: i16| {
            let (decimal, exponent) = positive_float_to_decimal(f);
            assert_eq!(decimal, decimal_expected, "unexpected decimal for positive_float_to_decimal({}); got {}; want {}", f, decimal, decimal_expected);
            assert_eq!(exponent, exponent_expected, "unexpected exponent for positive_float_to_decimal({}); got {}; want {}", f, exponent, exponent_expected);
        };

        test_case(0.0, 0, 1); // The exponent is 1 is OK here. See comment in positive_float_to_decimal.
        test_case(1.0, 1, 0);
        test_case(30.0, 3, 1);
        test_case(12345678900000000.0, 123456789, 8);
        test_case(12345678901234567.0, 12345678901234568, 0);
        test_case(1234567890123456789.0, 12345678901234567, 2);
        test_case(12345678901234567890.0, 12345678901234567, 3);
        test_case(18446744073670737131.0, 18446744073670737, 3);
     //   test_case(123456789012345678901.0, 12345678901234568, 4);
        // test_case((1 << 53) as f64, 1 << 53, 0);
        // test_case((1 << 54) as f64, 18014398509481984, 0);
        // test_case((1 << 55) as f64, 3602879701896396, 1);
        // test_case((1 << 62) as f64, 4611686018427387, 3);
        // test_case((1 << 63) as f64, 9223372036854775, 3);
        // Skip this test, since M1 returns 18446744073709551 instead of 18446744073709548
        // See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1653
        // test_case(1 << 64, 18446744073709548, 3);
        // test_case((1 << 65) as f64, 368934881474191, 5);
        // test_case((1 << 66) as f64, 737869762948382, 5);
        // test_case((1 << 67) as f64, 1475739525896764, 5);

       // test_case(0.1, 1, -1);
        test_case(123456789012345678e-5, 12345678901234568, -4);
        test_case(1234567890123456789e-10, 12345678901234568, -8);
        test_case(1234567890123456789e-14, 1234567890123, -8);
        test_case(1234567890123456789e-17, 12345678901234, -12);
        test_case(1234567890123456789e-20, 1234567890123, -14);

        test_case(0.000874957, 874957, -9);
        test_case(0.001130435, 1130435, -9);

        // Extreme cases. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/1114
        test_case(2.964393875e-100, 2964393875, -109);
        test_case(2.964393875e-309, 2964393875, -318);
        test_case(2.964393875e-314, 296439387505, -325);
        test_case(2.964393875e-315, 2964393875047, -327);
        test_case(2.964393875e-320, 296439387505, -331);
        test_case(2.964393875e-324, 494065645841, -335);
        test_case(2.964393875e-325, 0, 1);

        test_case(2.964393875e+307, 2964393875, 298);
        test_case(9.964393875e+307, 9964393875, 298);
        test_case(1.064393875e+308, 1064393875, 299);
        test_case(1.797393875e+308, 1797393875, 299);
    }
}