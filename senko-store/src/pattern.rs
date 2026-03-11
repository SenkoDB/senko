use memchr::memmem;

pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    if !has_wildcards(pattern) {
        return memmem::find(text, pattern).is_some();
    }
    glob_match_impl(pattern, text)
}

fn has_wildcards(pattern: &[u8]) -> bool {
    pattern
        .iter()
        .any(|b| matches!(b, b'*' | b'?' | b'[' | b']' | b'\\'))
}

fn glob_match_impl(pattern: &[u8], text: &[u8]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }
    match pattern[0] {
        b'*' => {
            let mut i = 0usize;
            while i <= text.len() {
                if glob_match_impl(&pattern[1..], &text[i..]) {
                    return true;
                }
                i += 1;
            }
            false
        }
        b'?' => !text.is_empty() && glob_match_impl(&pattern[1..], &text[1..]),
        b'[' => match_char_class(pattern, text),
        b'\\' => {
            if pattern.len() < 2 || text.is_empty() {
                return false;
            }
            pattern[1] == text[0] && glob_match_impl(&pattern[2..], &text[1..])
        }
        ch => !text.is_empty() && ch == text[0] && glob_match_impl(&pattern[1..], &text[1..]),
    }
}

fn match_char_class(pattern: &[u8], text: &[u8]) -> bool {
    if text.is_empty() {
        return false;
    }
    let mut idx = 1usize;
    let mut negated = false;
    if idx < pattern.len() && (pattern[idx] == b'^' || pattern[idx] == b'!') {
        negated = true;
        idx += 1;
    }
    let mut matched = false;
    while idx < pattern.len() {
        let ch = pattern[idx];
        if ch == b']' {
            let ok = if negated { !matched } else { matched };
            return ok && glob_match_impl(&pattern[idx + 1..], &text[1..]);
        }
        if ch == b'\\' && idx + 1 < pattern.len() {
            idx += 1;
            matched |= pattern[idx] == text[0];
            idx += 1;
            continue;
        }
        matched |= ch == text[0];
        idx += 1;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::glob_match;

    #[test]
    fn literal_patterns_use_substring_match() {
        assert!(glob_match(b"foo", b"foo"));
        assert!(glob_match(b"foo", b"prefixfoo"));
        assert!(!glob_match(b"foo", b"bar"));
    }

    #[test]
    fn wildcard_patterns_match() {
        assert!(glob_match(b"f*o", b"fooooo"));
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"[ab]", b"a"));
        assert!(!glob_match(b"[ab]", b"c"));
    }
}
