use bytes::Bytes;
use senko_core::{SenkoError, SenkoResult, SenkoValue};
use senko_proto::Frame;
use smallvec::SmallVec;

use crate::commands::Response;
use crate::store::Store;

const DEFAULT_MAX_CELLS: usize = 64_000_000;

#[derive(Debug, Clone, Copy)]
#[derive(Default)]
struct LcsOptions {
    len_only: bool,
    idx: bool,
    with_match_len: bool,
    min_match_len: usize,
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MatchRange {
    a_start: usize,
    a_end: usize,
    b_start: usize,
    b_end: usize,
    len: usize,
}

#[inline]
pub fn lcs(store: &mut Store, args: &[Frame<'_>]) -> SenkoResult<Response> {
    if args.len() < 2 {
        return Err(SenkoError::Protocol(
            "wrong number of arguments for 'lcs' command",
        ));
    }

    let key1 = arg_bytes(&args[0])?;
    let key2 = arg_bytes(&args[1])?;
    let options = parse_options(&args[2..])?;

    let left = value_wire_bytes(store.get(key1));
    let right = value_wire_bytes(store.get(key2));

    if options.len_only && !options.idx {
        let len = lcs_len(left.as_ref(), right.as_ref())?;
        return Ok(Response::Integer(len as i64));
    }

    let product = left
        .len()
        .checked_mul(right.len())
        .ok_or_else(memory_error)?;
    if product > DEFAULT_MAX_CELLS {
        return Err(memory_error());
    }

    let (lcs_bytes, matches) = lcs_full(left.as_ref(), right.as_ref())?;

    if options.idx {
        return Ok(render_idx_response(
            &matches,
            lcs_bytes.len(),
            options.min_match_len,
            options.with_match_len,
        ));
    }
    if options.len_only {
        return Ok(Response::Integer(lcs_bytes.len() as i64));
    }

    Ok(Response::Value(Some(SenkoValue::Raw(Bytes::from(
        lcs_bytes,
    )))))
}

fn parse_options(args: &[Frame<'_>]) -> SenkoResult<LcsOptions> {
    let mut options = LcsOptions::default();
    let mut index = 0usize;

    while index < args.len() {
        let flag = arg_bytes(&args[index])?;
        if is_opt(flag, b"LEN") {
            options.len_only = true;
            index += 1;
            continue;
        }
        if is_opt(flag, b"IDX") {
            options.idx = true;
            index += 1;
            continue;
        }
        if is_opt(flag, b"WITHMATCHLEN") {
            options.with_match_len = true;
            index += 1;
            continue;
        }
        if is_opt(flag, b"MINMATCHLEN") {
            index += 1;
            if index >= args.len() {
                return Err(SenkoError::Protocol("syntax error"));
            }
            let value = parse_usize(arg_bytes(&args[index])?)
                .ok_or_else(|| SenkoError::Protocol("syntax error"))?;
            options.min_match_len = value;
            index += 1;
            continue;
        }
        return Err(SenkoError::Protocol("syntax error"));
    }

    if options.with_match_len && !options.idx {
        return Err(SenkoError::Protocol("syntax error"));
    }

    Ok(options)
}

fn lcs_len(a: &[u8], b: &[u8]) -> SenkoResult<usize> {
    if let Some(value) = simd::lcs_len_simd(a, b) {
        return Ok(value);
    }
    lcs_len_scalar(a, b)
}

fn lcs_len_scalar(a: &[u8], b: &[u8]) -> SenkoResult<usize> {
    if a.is_empty() || b.is_empty() {
        return Ok(0);
    }

    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };

    let mut prev = vec![0u16; short.len() + 1];
    let mut curr = vec![0u16; short.len() + 1];

    for &lc in long {
        curr[0] = 0;
        for (j, &sc) in short.iter().enumerate() {
            curr[j + 1] = if lc == sc {
                prev[j].saturating_add(1)
            } else {
                prev[j + 1].max(curr[j])
            };
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    Ok(prev[short.len()] as usize)
}

mod simd {
    #[cfg(target_arch = "aarch64")]
    use core::arch::aarch64::{vceqq_u8, vdupq_n_u8, vld1q_u8, vst1q_u8};
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::*;

    #[allow(unsafe_code)]
    pub(super) fn lcs_len_simd(a: &[u8], b: &[u8]) -> Option<usize> {
        if a.is_empty() || b.is_empty() {
            return Some(0);
        }

        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx512bw") {
                // SAFETY: Called only when AVX-512BW support is detected at runtime.
                return Some(unsafe { lcs_len_x86_avx512bw(a, b) });
            }
            if std::is_x86_feature_detected!("avx2") {
                // SAFETY: Called only when AVX2 support is detected at runtime.
                return Some(unsafe { lcs_len_x86_avx2(a, b) });
            }
            if std::is_x86_feature_detected!("sse4.2") {
                // SAFETY: Called only when SSE4.2 support is detected at runtime.
                return Some(unsafe { lcs_len_x86_sse42(a, b) });
            }
        }

        #[cfg(target_arch = "aarch64")]
        {
            if std::arch::is_aarch64_feature_detected!("neon") {
                // SAFETY: Called only when NEON support is detected at runtime.
                return Some(unsafe { lcs_len_neon(a, b) });
            }
        }

        None
    }

    #[allow(unsafe_code)]
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.2")]
    unsafe fn lcs_len_x86_sse42(a: &[u8], b: &[u8]) -> usize {
        let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
        let mut prev = vec![0u16; short.len() + 1];
        let mut curr = vec![0u16; short.len() + 1];
        let mut eq = vec![0u8; short.len()];

        for &lc in long {
            // SAFETY: Intrinsics require target feature and valid pointers into `short`.
            unsafe { fill_eq_mask_sse42(short, lc, &mut eq) };
            curr[0] = 0;
            for j in 0..short.len() {
                curr[j + 1] = if eq[j] != 0 {
                    prev[j].saturating_add(1)
                } else {
                    prev[j + 1].max(curr[j])
                };
            }
            std::mem::swap(&mut prev, &mut curr);
        }

        prev[short.len()] as usize
    }

    #[allow(unsafe_code)]
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    unsafe fn lcs_len_x86_avx2(a: &[u8], b: &[u8]) -> usize {
        let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
        let mut prev = vec![0u16; short.len() + 1];
        let mut curr = vec![0u16; short.len() + 1];
        let mut eq = vec![0u8; short.len()];

        for &lc in long {
            // SAFETY: Intrinsics require target feature and valid pointers into `short`.
            unsafe { fill_eq_mask_avx2(short, lc, &mut eq) };
            curr[0] = 0;
            for j in 0..short.len() {
                curr[j + 1] = if eq[j] != 0 {
                    prev[j].saturating_add(1)
                } else {
                    prev[j + 1].max(curr[j])
                };
            }
            std::mem::swap(&mut prev, &mut curr);
        }

        prev[short.len()] as usize
    }

    #[allow(unsafe_code)]
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512bw")]
    unsafe fn lcs_len_x86_avx512bw(a: &[u8], b: &[u8]) -> usize {
        let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
        let mut prev = vec![0u16; short.len() + 1];
        let mut curr = vec![0u16; short.len() + 1];
        let mut eq = vec![0u8; short.len()];

        for &lc in long {
            // SAFETY: Intrinsics require target feature and valid pointers into `short`.
            unsafe { fill_eq_mask_avx512bw(short, lc, &mut eq) };
            curr[0] = 0;
            for j in 0..short.len() {
                curr[j + 1] = if eq[j] != 0 {
                    prev[j].saturating_add(1)
                } else {
                    prev[j + 1].max(curr[j])
                };
            }
            std::mem::swap(&mut prev, &mut curr);
        }

        prev[short.len()] as usize
    }

    #[allow(unsafe_code)]
    #[cfg(target_arch = "aarch64")]
    unsafe fn lcs_len_neon(a: &[u8], b: &[u8]) -> usize {
        let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
        let mut prev = vec![0u16; short.len() + 1];
        let mut curr = vec![0u16; short.len() + 1];
        let mut eq = vec![0u8; short.len()];

        for &lc in long {
            // SAFETY: Intrinsics require target feature and valid pointers into `short`.
            unsafe { fill_eq_mask_neon(short, lc, &mut eq) };
            curr[0] = 0;
            for j in 0..short.len() {
                curr[j + 1] = if eq[j] != 0 {
                    prev[j].saturating_add(1)
                } else {
                    prev[j + 1].max(curr[j])
                };
            }
            std::mem::swap(&mut prev, &mut curr);
        }

        prev[short.len()] as usize
    }

    #[allow(unsafe_code)]
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "sse4.2")]
    unsafe fn fill_eq_mask_sse42(short: &[u8], byte: u8, out: &mut [u8]) {
        let mut i = 0usize;
        let needle = _mm_set1_epi8(byte as i8);
        while i + 16 <= short.len() {
            // SAFETY: We read exactly 16 bytes from a valid slice range.
            let chunk = unsafe { _mm_loadu_si128(short.as_ptr().add(i).cast::<__m128i>()) };
            let cmp = _mm_cmpeq_epi8(chunk, needle);
            let mask = _mm_movemask_epi8(cmp) as u32;
            for lane in 0..16 {
                out[i + lane] = ((mask >> lane) & 1) as u8;
            }
            i += 16;
        }
        for (dst, &src) in out[i..].iter_mut().zip(&short[i..]) {
            *dst = u8::from(src == byte);
        }
    }

    #[allow(unsafe_code)]
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx2")]
    unsafe fn fill_eq_mask_avx2(short: &[u8], byte: u8, out: &mut [u8]) {
        let mut i = 0usize;
        let needle = _mm256_set1_epi8(byte as i8);
        while i + 32 <= short.len() {
            // SAFETY: We read exactly 32 bytes from a valid slice range.
            let chunk = unsafe { _mm256_loadu_si256(short.as_ptr().add(i).cast::<__m256i>()) };
            let cmp = _mm256_cmpeq_epi8(chunk, needle);
            let mask = _mm256_movemask_epi8(cmp) as u32;
            for lane in 0..32 {
                out[i + lane] = ((mask >> lane) & 1) as u8;
            }
            i += 32;
        }
        for (dst, &src) in out[i..].iter_mut().zip(&short[i..]) {
            *dst = u8::from(src == byte);
        }
    }

    #[allow(unsafe_code)]
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512bw")]
    unsafe fn fill_eq_mask_avx512bw(short: &[u8], byte: u8, out: &mut [u8]) {
        let mut i = 0usize;
        let needle = _mm512_set1_epi8(byte as i8);
        while i + 64 <= short.len() {
            // SAFETY: We read exactly 64 bytes from a valid slice range.
            let chunk = unsafe { _mm512_loadu_si512(short.as_ptr().add(i).cast()) };
            let mask = _mm512_cmpeq_epi8_mask(chunk, needle);
            for lane in 0..64 {
                out[i + lane] = ((mask >> lane) & 1) as u8;
            }
            i += 64;
        }
        for (dst, &src) in out[i..].iter_mut().zip(&short[i..]) {
            *dst = u8::from(src == byte);
        }
    }

    #[allow(unsafe_code)]
    #[cfg(target_arch = "aarch64")]
    unsafe fn fill_eq_mask_neon(short: &[u8], byte: u8, out: &mut [u8]) {
        let mut i = 0usize;
        let needle = vdupq_n_u8(byte);
        while i + 16 <= short.len() {
            // SAFETY: We read and write exactly 16 bytes from valid slice ranges.
            let chunk = unsafe { vld1q_u8(short.as_ptr().add(i)) };
            let cmp = vceqq_u8(chunk, needle);
            // SAFETY: `cmp` is a 16-byte vector, and `out` has at least 16 bytes available.
            unsafe { vst1q_u8(out.as_mut_ptr().add(i), cmp) };
            for lane in 0..16 {
                out[i + lane] = u8::from(out[i + lane] != 0);
            }
            i += 16;
        }
        for (dst, &src) in out[i..].iter_mut().zip(&short[i..]) {
            *dst = u8::from(src == byte);
        }
    }
}

fn lcs_full(a: &[u8], b: &[u8]) -> SenkoResult<(Vec<u8>, Vec<MatchRange>)> {
    let rows = a.len() + 1;
    let cols = b.len() + 1;
    let mut dp = vec![0u16; rows * cols];

    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let idx = i * cols + j;
            dp[idx] = if a[i - 1] == b[j - 1] {
                dp[(i - 1) * cols + (j - 1)].saturating_add(1)
            } else {
                dp[(i - 1) * cols + j].max(dp[i * cols + (j - 1)])
            };
        }
    }

    let mut i = a.len();
    let mut j = b.len();
    let mut pairs = Vec::with_capacity(dp[a.len() * cols + b.len()] as usize);

    while i > 0 && j > 0 {
        if a[i - 1] == b[j - 1] {
            pairs.push((i - 1, j - 1));
            i -= 1;
            j -= 1;
        } else {
            let up = dp[(i - 1) * cols + j];
            let left = dp[i * cols + (j - 1)];
            if up >= left {
                i -= 1;
            } else {
                j -= 1;
            }
        }
    }

    pairs.reverse();

    let mut lcs = Vec::with_capacity(pairs.len());
    for (ai, _) in &pairs {
        lcs.push(a[*ai]);
    }

    let mut ranges = Vec::new();
    if !pairs.is_empty() {
        let (mut a_start, mut b_start) = pairs[0];
        let mut a_prev = a_start;
        let mut b_prev = b_start;

        for &(ai, bi) in pairs.iter().skip(1) {
            if ai == a_prev + 1 && bi == b_prev + 1 {
                a_prev = ai;
                b_prev = bi;
            } else {
                ranges.push(MatchRange {
                    a_start,
                    a_end: a_prev,
                    b_start,
                    b_end: b_prev,
                    len: a_prev - a_start + 1,
                });
                a_start = ai;
                b_start = bi;
                a_prev = ai;
                b_prev = bi;
            }
        }

        ranges.push(MatchRange {
            a_start,
            a_end: a_prev,
            b_start,
            b_end: b_prev,
            len: a_prev - a_start + 1,
        });
    }

    Ok((lcs, ranges))
}

fn render_idx_response(
    ranges: &[MatchRange],
    total_len: usize,
    min_match_len: usize,
    with_match_len: bool,
) -> Response {
    let mut matches = SmallVec::<[Response; 16]>::new();

    for range in ranges {
        if range.len < min_match_len {
            continue;
        }

        let mut entry = SmallVec::<[Response; 16]>::new();
        entry.push(Response::Array(Box::new(SmallVec::from_buf([
            Response::Integer(range.a_start as i64),
            Response::Integer(range.a_end as i64),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
        ]))));
        if let Response::Array(inner) = &mut entry[0] {
            inner.truncate(2);
        }

        entry.push(Response::Array(Box::new(SmallVec::from_buf([
            Response::Integer(range.b_start as i64),
            Response::Integer(range.b_end as i64),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
            Response::Integer(0),
        ]))));
        if let Response::Array(inner) = &mut entry[1] {
            inner.truncate(2);
        }

        if with_match_len {
            entry.push(Response::Integer(range.len as i64));
        }

        matches.push(Response::Array(Box::new(entry)));
    }

    let mut top = SmallVec::<[Response; 16]>::new();
    top.push(Response::Value(Some(SenkoValue::Raw(Bytes::from_static(
        b"matches",
    )))));
    top.push(Response::Array(Box::new(matches)));
    top.push(Response::Value(Some(SenkoValue::Raw(Bytes::from_static(
        b"len",
    )))));
    top.push(Response::Integer(total_len as i64));
    Response::Array(Box::new(top))
}

fn memory_error() -> SenkoError {
    SenkoError::Protocol("ERR LCS requires too much memory")
}

fn value_wire_bytes(value: Option<&SenkoValue>) -> Bytes {
    match value {
        None => Bytes::new(),
        Some(SenkoValue::Raw(bytes)) => bytes.clone(),
        Some(SenkoValue::Int(v)) => Bytes::from(v.to_string()),
        Some(SenkoValue::Float(v)) => Bytes::from(v.to_string()),
        Some(SenkoValue::Hash(_)) => Bytes::from_static(b"[hash]"),
        Some(SenkoValue::List(_)) => Bytes::from_static(b"[list]"),
        Some(SenkoValue::Set(_)) => Bytes::from_static(b"[set]"),
        Some(SenkoValue::Stream(_)) => Bytes::from_static(b"[stream]"),
        Some(SenkoValue::ZSet(_)) => Bytes::from_static(b"[zset]"),
        #[cfg(feature = "prob")]
        Some(SenkoValue::BloomFilter(_)) => Bytes::from_static(b"[bloom]"),
        #[cfg(feature = "prob")]
        Some(SenkoValue::CuckooFilter(_)) => Bytes::from_static(b"[cuckoo]"),
        #[cfg(feature = "prob")]
        Some(SenkoValue::CountMinSketch(_)) => Bytes::from_static(b"[cms]"),
        #[cfg(feature = "prob")]
        Some(SenkoValue::TopK(_)) => Bytes::from_static(b"[topk]"),
        #[cfg(feature = "prob")]
        Some(SenkoValue::TDigest(_)) => Bytes::from_static(b"[tdigest]"),
        #[cfg(feature = "json")]
        Some(SenkoValue::Json(_)) => Bytes::from_static(b"[json]"),
        #[cfg(feature = "vector")]
        Some(SenkoValue::VectorSet(_)) => Bytes::from_static(b"[vectorset]"),
    }
}

fn arg_bytes<'a>(frame: &'a Frame<'_>) -> SenkoResult<&'a [u8]> {
    match frame {
        Frame::BulkString(bytes) | Frame::SimpleString(bytes) => Ok(bytes),
        Frame::VerbatimString { data, .. } => Ok(data),
        _ => Err(SenkoError::WrongType {
            expected: "string",
            actual: frame_type_name(frame),
        }),
    }
}

fn parse_usize(raw: &[u8]) -> Option<usize> {
    std::str::from_utf8(raw).ok()?.parse::<usize>().ok()
}

fn is_opt(input: &[u8], expected_upper: &[u8]) -> bool {
    input.eq_ignore_ascii_case(expected_upper)
}

fn frame_type_name(frame: &Frame<'_>) -> &'static str {
    match frame {
        Frame::SimpleString(_) => "simple-string",
        Frame::SimpleError(_) => "simple-error",
        Frame::Integer(_) => "integer",
        Frame::BulkString(_) => "bulk-string",
        Frame::Array(_) => "array",
        Frame::Null => "null",
        Frame::Boolean(_) => "boolean",
        Frame::Double(_) => "double",
        Frame::BigNumber(_) => "big-number",
        Frame::BlobError(_) => "blob-error",
        Frame::VerbatimString { .. } => "verbatim-string",
        Frame::Map(_) => "map",
        Frame::Set(_) => "set",
        Frame::Push(_) => "push",
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use compact_str::CompactString;
    use proptest::prelude::*;
    use senko_core::SenkoValue;
    use senko_proto::Frame;

    use crate::{
        commands::{Response, lcs},
        store::{SetOptions, Store},
    };

    fn bs<'a>(input: &'a [u8]) -> Frame<'a> {
        Frame::BulkString(input)
    }

    fn set_raw(store: &mut Store, key: &str, value: &[u8]) {
        let _ = store.set(
            CompactString::from(key),
            SenkoValue::Raw(Bytes::copy_from_slice(value)),
            SetOptions::default(),
        );
    }

    #[test]
    fn lcs_known_example() {
        let mut store = Store::default();
        set_raw(&mut store, "a", b"ohmytext");
        set_raw(&mut store, "b", b"mynewtext");
        let res = lcs::lcs(&mut store, &[bs(b"a"), bs(b"b")]).unwrap();
        assert_eq!(
            res,
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b"mytext"))))
        );
    }

    #[test]
    fn lcs_len_flag() {
        let mut store = Store::default();
        set_raw(&mut store, "a", b"ohmytext");
        set_raw(&mut store, "b", b"mynewtext");
        let res = lcs::lcs(&mut store, &[bs(b"a"), bs(b"b"), bs(b"LEN")]).unwrap();
        assert_eq!(res, Response::Integer(6));
    }

    #[test]
    fn lcs_idx_with_matchlen() {
        let mut store = Store::default();
        set_raw(&mut store, "a", b"abcdef");
        set_raw(&mut store, "b", b"abXXef");
        let res = lcs::lcs(
            &mut store,
            &[bs(b"a"), bs(b"b"), bs(b"IDX"), bs(b"WITHMATCHLEN")],
        )
        .unwrap();
        assert!(matches!(res, Response::Array(_)));
    }

    #[test]
    fn lcs_minmatchlen_filtering() {
        let mut store = Store::default();
        set_raw(&mut store, "a", b"abZZcd");
        set_raw(&mut store, "b", b"abYYcd");
        let res = lcs::lcs(
            &mut store,
            &[bs(b"a"), bs(b"b"), bs(b"IDX"), bs(b"MINMATCHLEN"), bs(b"3")],
        )
        .unwrap();
        if let Response::Array(top) = res {
            assert_eq!(top[3], Response::Integer(4));
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn lcs_missing_key_treated_empty() {
        let mut store = Store::default();
        set_raw(&mut store, "a", b"abc");
        let res = lcs::lcs(&mut store, &[bs(b"a"), bs(b"missing")]).unwrap();
        assert_eq!(
            res,
            Response::Value(Some(SenkoValue::Raw(Bytes::from_static(b""))))
        );
    }

    fn lcs_len_scalar(a: &[u8], b: &[u8]) -> usize {
        lcs::lcs_len_scalar(a, b).unwrap()
    }

    fn lcs_len_auto(a: &[u8], b: &[u8]) -> usize {
        lcs::lcs_len(a, b).unwrap()
    }

    proptest! {
        #[test]
        fn rolling_len_matches_full_reconstruction(a in proptest::collection::vec(any::<u8>(), 0..64), b in proptest::collection::vec(any::<u8>(), 0..64)) {
            let rolling = lcs_len_scalar(&a, &b);
            let (full_lcs, _) = lcs::lcs_full(&a, &b).unwrap();
            prop_assert_eq!(rolling, full_lcs.len());
        }

        #[test]
        fn simd_len_matches_scalar(a in proptest::collection::vec(any::<u8>(), 0..128), b in proptest::collection::vec(any::<u8>(), 0..128)) {
            let scalar = lcs_len_scalar(&a, &b);
            let auto = lcs_len_auto(&a, &b);
            prop_assert_eq!(auto, scalar);
        }
    }
}
