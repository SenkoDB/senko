use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

fn lcs_len_scalar(a: &[u8], b: &[u8]) -> usize {
    if a.is_empty() || b.is_empty() {
        return 0;
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
    prev[short.len()] as usize
}

fn lcs_len_sse42(a: &[u8], b: &[u8]) -> usize {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("sse4.2") {
            // SAFETY: Guarded by runtime CPU feature check.
            return unsafe { lcs_len_x86_sse42(a, b) };
        }
    }
    lcs_len_scalar(a, b)
}

fn lcs_len_avx2(a: &[u8], b: &[u8]) -> usize {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            // SAFETY: Guarded by runtime CPU feature check.
            return unsafe { lcs_len_x86_avx2(a, b) };
        }
    }
    lcs_len_scalar(a, b)
}

fn lcs_len_avx512(a: &[u8], b: &[u8]) -> usize {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx512bw") {
            // SAFETY: Guarded by runtime CPU feature check.
            return unsafe { lcs_len_x86_avx512bw(a, b) };
        }
    }
    lcs_len_scalar(a, b)
}

#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::*;

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn lcs_len_x86_sse42(a: &[u8], b: &[u8]) -> usize {
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    let mut prev = vec![0u16; short.len() + 1];
    let mut curr = vec![0u16; short.len() + 1];
    let mut eq = vec![0u8; short.len()];

    for &lc in long {
        // SAFETY: Requires sse4.2 feature and valid slice bounds.
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

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn lcs_len_x86_avx2(a: &[u8], b: &[u8]) -> usize {
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    let mut prev = vec![0u16; short.len() + 1];
    let mut curr = vec![0u16; short.len() + 1];
    let mut eq = vec![0u8; short.len()];

    for &lc in long {
        // SAFETY: Requires avx2 feature and valid slice bounds.
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

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw")]
unsafe fn lcs_len_x86_avx512bw(a: &[u8], b: &[u8]) -> usize {
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    let mut prev = vec![0u16; short.len() + 1];
    let mut curr = vec![0u16; short.len() + 1];
    let mut eq = vec![0u8; short.len()];

    for &lc in long {
        // SAFETY: Requires avx512bw feature and valid slice bounds.
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

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn fill_eq_mask_sse42(short: &[u8], byte: u8, out: &mut [u8]) {
    let mut i = 0usize;
    let needle = _mm_set1_epi8(byte as i8);
    while i + 16 <= short.len() {
        // SAFETY: Loads 16 bytes from an in-bounds slice region.
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

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn fill_eq_mask_avx2(short: &[u8], byte: u8, out: &mut [u8]) {
    let mut i = 0usize;
    let needle = _mm256_set1_epi8(byte as i8);
    while i + 32 <= short.len() {
        // SAFETY: Loads 32 bytes from an in-bounds slice region.
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

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw")]
unsafe fn fill_eq_mask_avx512bw(short: &[u8], byte: u8, out: &mut [u8]) {
    let mut i = 0usize;
    let needle = _mm512_set1_epi8(byte as i8);
    while i + 64 <= short.len() {
        // SAFETY: Loads 64 bytes from an in-bounds slice region.
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

fn bench_lcs(c: &mut Criterion) {
    let a = vec![b'a'; 1000];
    let b = vec![b'a'; 1000];

    let mut group = c.benchmark_group("lcs_1000x1000");
    group.bench_with_input(
        BenchmarkId::new("scalar", 1000),
        &(a.as_slice(), b.as_slice()),
        |bencher, (left, right)| {
            bencher.iter(|| lcs_len_scalar(left, right));
        },
    );
    group.bench_with_input(
        BenchmarkId::new("sse4.2", 1000),
        &(a.as_slice(), b.as_slice()),
        |bencher, (left, right)| {
            bencher.iter(|| lcs_len_sse42(left, right));
        },
    );
    group.bench_with_input(
        BenchmarkId::new("avx2", 1000),
        &(a.as_slice(), b.as_slice()),
        |bencher, (left, right)| {
            bencher.iter(|| lcs_len_avx2(left, right));
        },
    );
    group.bench_with_input(
        BenchmarkId::new("avx512", 1000),
        &(a.as_slice(), b.as_slice()),
        |bencher, (left, right)| {
            bencher.iter(|| lcs_len_avx512(left, right));
        },
    );
    group.finish();
}

criterion_group!(benches, bench_lcs);
criterion_main!(benches);
