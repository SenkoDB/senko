use std::sync::Arc;

use aho_corasick::{AhoCorasick, AhoCorasickBuilder};
use compact_str::CompactString;
use memchr::memmem;
use smallvec::SmallVec;

use crate::{
    message::{MessageKind, PubSubMessage},
    slot::BroadcastSlot,
};

const AHO_THRESHOLD: usize = 32;

#[derive(Debug, Clone)]
pub struct PatternSubscription {
    pub pattern: CompactString,
    pub slots: SmallVec<[Arc<BroadcastSlot>; 4]>,
    pub subscriber_count: u32,
}

impl PatternSubscription {
    #[inline]
    pub fn new(pattern: CompactString) -> Self {
        Self {
            pattern,
            slots: SmallVec::new(),
            subscriber_count: 0,
        }
    }

    #[inline]
    pub fn subscribe(&mut self, conn_id: u64) -> Arc<BroadcastSlot> {
        if let Some(existing) = self.slots.iter().find(|slot| slot.conn_id() == conn_id) {
            return Arc::clone(existing);
        }

        let slot = Arc::new(BroadcastSlot::new(conn_id));
        self.slots.push(Arc::clone(&slot));
        self.subscriber_count = self.slots.len() as u32;
        slot
    }

    #[inline]
    pub fn unsubscribe(&mut self, conn_id: u64) -> Option<Arc<BroadcastSlot>> {
        let index = self
            .slots
            .iter()
            .position(|slot| slot.conn_id() == conn_id)?;
        let slot = self.slots.swap_remove(index);
        self.subscriber_count = self.slots.len() as u32;
        Some(slot)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

#[derive(Debug, Default)]
pub struct PatternIndex {
    pub subscriptions: Vec<PatternSubscription>,
    matcher: Option<AhoCorasick>,
    literal_prefixes: Vec<(CompactString, usize)>,
    literal_to_pattern: Vec<usize>,
    literalless_patterns: SmallVec<[usize; 4]>,
    total_subscriber_count: u32,
}

impl PatternIndex {
    #[inline]
    pub fn subscribe(&mut self, pattern: &[u8], conn_id: u64) -> Arc<BroadcastSlot> {
        let pattern = pattern_name(pattern);
        let slot = if let Some(existing) = self
            .subscriptions
            .iter_mut()
            .find(|subscription| subscription.pattern == pattern)
        {
            existing.subscribe(conn_id)
        } else {
            let mut subscription = PatternSubscription::new(pattern);
            let slot = subscription.subscribe(conn_id);
            self.subscriptions.push(subscription);
            slot
        };
        self.total_subscriber_count = self
            .subscriptions
            .iter()
            .map(|subscription| subscription.subscriber_count)
            .sum();
        self.rebuild();
        slot
    }

    #[inline]
    pub fn unsubscribe(&mut self, pattern: &[u8], conn_id: u64) -> Option<Arc<BroadcastSlot>> {
        let pattern = pattern_name(pattern);
        let index = self
            .subscriptions
            .iter()
            .position(|subscription| subscription.pattern == pattern)?;
        let slot = self.subscriptions[index].unsubscribe(conn_id)?;
        if self.subscriptions[index].is_empty() {
            self.subscriptions.swap_remove(index);
        }
        self.total_subscriber_count = self
            .subscriptions
            .iter()
            .map(|subscription| subscription.subscriber_count)
            .sum();
        self.rebuild();
        Some(slot)
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.subscriptions.len()
    }

    #[inline(always)]
    pub fn total_subscriber_count(&self) -> u32 {
        self.total_subscriber_count
    }

    #[inline]
    pub fn has_matching_pattern(&self, channel: &[u8]) -> bool {
        self.candidate_pattern_indexes(channel)
            .into_iter()
            .any(|index| glob_match_simd(self.subscriptions[index].pattern.as_bytes(), channel))
    }

    pub fn publish_to_patterns(&mut self, channel: &[u8], msg: Arc<PubSubMessage>) -> u64 {
        if self.subscriptions.is_empty() {
            return 0;
        }

        let candidate_indexes = self.candidate_pattern_indexes(channel);
        if candidate_indexes.is_empty() {
            return 0;
        }

        let mut delivered = 0u64;
        let mut lagged = SmallVec::<[(CompactString, u64); 8]>::new();
        let channel_name = msg.channel.clone();
        let payload = msg.payload.clone();

        for index in candidate_indexes {
            let subscription = &self.subscriptions[index];
            if !glob_match_simd(subscription.pattern.as_bytes(), channel) {
                continue;
            }

            let pmessage = Arc::new(PubSubMessage {
                channel: channel_name.clone(),
                payload: payload.clone(),
                kind: MessageKind::PMessage {
                    pattern: subscription.pattern.clone(),
                },
            });

            for slot in &subscription.slots {
                match slot.publish(Arc::clone(&pmessage)) {
                    Ok(()) => delivered += 1,
                    Err(_) => lagged.push((subscription.pattern.clone(), slot.conn_id())),
                }
            }
        }

        for (pattern, conn_id) in lagged {
            let _ = self.unsubscribe(pattern.as_bytes(), conn_id);
        }

        delivered
    }

    fn candidate_pattern_indexes(&self, channel: &[u8]) -> SmallVec<[usize; 16]> {
        if self.subscriptions.is_empty() {
            return SmallVec::new();
        }

        if self.subscriptions.len() <= AHO_THRESHOLD {
            return (0..self.subscriptions.len()).collect();
        }

        let mut candidate_indexes = SmallVec::<[usize; 16]>::new();
        candidate_indexes.extend(self.literalless_patterns.iter().copied());

        for (prefix, pattern_index) in &self.literal_prefixes {
            if !prefix.is_empty() && channel.starts_with(prefix.as_bytes()) {
                candidate_indexes.push(*pattern_index);
            }
        }

        if let Some(matcher) = &self.matcher {
            for mat in matcher.find_overlapping_iter(channel) {
                let pattern_index = self.literal_to_pattern[mat.pattern().as_usize()];
                candidate_indexes.push(pattern_index);
            }
        }

        candidate_indexes.sort_unstable();
        candidate_indexes.dedup();
        candidate_indexes
    }

    fn rebuild(&mut self) {
        self.matcher = None;
        self.literal_prefixes.clear();
        self.literal_to_pattern.clear();
        self.literalless_patterns.clear();

        if self.subscriptions.len() <= AHO_THRESHOLD {
            return;
        }

        let mut literals = Vec::<String>::new();
        for (pattern_index, subscription) in self.subscriptions.iter().enumerate() {
            let prefix = literal_prefix(subscription.pattern.as_bytes());
            if !prefix.is_empty() {
                self.literal_prefixes.push((prefix, pattern_index));
            }

            let segments = literal_segments(subscription.pattern.as_bytes());
            if segments.is_empty() {
                self.literalless_patterns.push(pattern_index);
                continue;
            }

            for segment in segments {
                self.literal_to_pattern.push(pattern_index);
                literals.push(segment);
            }
        }

        if !literals.is_empty() {
            let automaton = AhoCorasickBuilder::new()
                .build(&literals)
                .expect("aho-corasick build must succeed");
            self.matcher = Some(automaton);
        }
    }
}

#[inline(always)]
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    glob_match_simd(pattern, text)
}

pub fn glob_match_simd(pattern: &[u8], text: &[u8]) -> bool {
    if pattern == b"*" {
        return true;
    }

    let features = scan_pattern(pattern);
    if !features.has_wildcards {
        return pattern_eq_literal(pattern, text);
    }

    if let Some(prefix_len) = features.trailing_star_prefix_len {
        return text.starts_with(&pattern[..prefix_len]);
    }

    if let Some(segments) = literal_segments_for_prune(pattern, &features)
        && !segments_match_in_order(segments.as_slice(), text)
    {
        return false;
    }

    glob_match_scalar(pattern, text)
}

fn glob_match_scalar(pattern: &[u8], text: &[u8]) -> bool {
    let mut pattern_index = 0usize;
    let mut text_index = 0usize;
    let mut star_pattern = None;
    let mut star_text = 0usize;

    loop {
        if pattern_index == pattern.len() {
            if text_index == text.len() {
                return true;
            }
            if let Some(next_pattern) = star_pattern {
                star_text += 1;
                if star_text > text.len() {
                    return false;
                }
                text_index = star_text;
                pattern_index = next_pattern;
                continue;
            }
            return false;
        }

        match pattern[pattern_index] {
            b'*' => {
                while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
                    pattern_index += 1;
                }
                if pattern_index == pattern.len() {
                    return true;
                }
                star_pattern = Some(pattern_index);
                star_text = text_index;
            }
            _ => {
                if text_index < text.len() {
                    let (matched, token_len) =
                        match_single_token(pattern, pattern_index, text[text_index]);
                    if matched {
                        pattern_index += token_len;
                        text_index += 1;
                        continue;
                    }
                }

                if let Some(next_pattern) = star_pattern {
                    star_text += 1;
                    if star_text > text.len() {
                        return false;
                    }
                    text_index = star_text;
                    pattern_index = next_pattern;
                } else {
                    return false;
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct PatternFeatures {
    has_wildcards: bool,
    trailing_star_prefix_len: Option<usize>,
}

fn scan_pattern(pattern: &[u8]) -> PatternFeatures {
    let mut has_wildcards = false;
    let mut wildcard_count = 0usize;
    let mut star_at_end = false;
    let mut prefix_len = pattern.len();
    let mut index = 0usize;

    while index < pattern.len() {
        match pattern[index] {
            b'\\' => {
                if index + 1 < pattern.len() {
                    index += 2;
                } else {
                    index += 1;
                }
            }
            b'*' => {
                has_wildcards = true;
                wildcard_count += 1;
                star_at_end = index + 1 == pattern.len();
                prefix_len = index;
                index += 1;
            }
            b'?' | b'[' => {
                has_wildcards = true;
                wildcard_count += 1;
                index += if pattern[index] == b'[' {
                    char_class_token_len(pattern, index)
                } else {
                    1
                };
            }
            _ => index += 1,
        }
    }

    let trailing_star_prefix_len = if wildcard_count == 1 && star_at_end {
        Some(prefix_len)
    } else {
        None
    };

    PatternFeatures {
        has_wildcards,
        trailing_star_prefix_len,
    }
}

fn pattern_eq_literal(pattern: &[u8], text: &[u8]) -> bool {
    if !pattern.contains(&b'\\') {
        return pattern == text;
    }

    let mut literal = Vec::with_capacity(pattern.len());
    let mut index = 0usize;
    while index < pattern.len() {
        if pattern[index] == b'\\' && index + 1 < pattern.len() {
            literal.push(pattern[index + 1]);
            index += 2;
        } else {
            literal.push(pattern[index]);
            index += 1;
        }
    }
    literal.as_slice() == text
}

fn literal_segments_for_prune<'a>(
    pattern: &'a [u8],
    features: &PatternFeatures,
) -> Option<SmallVec<[&'a [u8]; 8]>> {
    if !features.has_wildcards {
        return None;
    }

    let mut segments = SmallVec::<[&[u8]; 8]>::new();
    let mut segment_start = None;
    let mut index = 0usize;

    while index < pattern.len() {
        match pattern[index] {
            b'\\' => {
                if segment_start.is_none() {
                    segment_start = Some(index);
                }
                index += (index + 1 < pattern.len()) as usize + 1;
            }
            b'*' | b'?' | b'[' => {
                if let Some(start) = segment_start.take()
                    && index > start
                {
                    segments.push(&pattern[start..index]);
                }
                index += if pattern[index] == b'[' {
                    char_class_token_len(pattern, index)
                } else {
                    1
                };
            }
            _ => {
                if segment_start.is_none() {
                    segment_start = Some(index);
                }
                index += 1;
            }
        }
    }

    if let Some(start) = segment_start {
        segments.push(&pattern[start..]);
    }

    if segments.is_empty() {
        None
    } else {
        Some(segments)
    }
}

fn segments_match_in_order(segments: &[&[u8]], text: &[u8]) -> bool {
    let mut remaining = text;
    for segment in segments {
        let literal = unescape_segment(segment);
        let Some(position) = memmem::find(remaining, literal.as_slice()) else {
            return false;
        };
        remaining = &remaining[position + literal.len()..];
    }
    true
}

fn match_single_token(pattern: &[u8], index: usize, byte: u8) -> (bool, usize) {
    match pattern[index] {
        b'?' => (true, 1),
        b'[' => match_char_class(&pattern[index..], byte),
        b'\\' => {
            if index + 1 < pattern.len() {
                (pattern[index + 1] == byte, 2)
            } else {
                (b'\\' == byte, 1)
            }
        }
        literal => (literal == byte, 1),
    }
}

fn match_char_class(pattern: &[u8], byte: u8) -> (bool, usize) {
    let mut index = 1usize;
    let mut negated = false;
    if index < pattern.len() && (pattern[index] == b'^' || pattern[index] == b'!') {
        negated = true;
        index += 1;
    }

    let mut matched = false;
    while index < pattern.len() {
        match pattern[index] {
            b']' => {
                let ok = if negated { !matched } else { matched };
                return (ok, index + 1);
            }
            b'\\' if index + 1 < pattern.len() => {
                matched |= pattern[index + 1] == byte;
                index += 2;
            }
            start
                if index + 2 < pattern.len()
                    && pattern[index + 1] == b'-'
                    && pattern[index + 2] != b']' =>
            {
                let end = pattern[index + 2];
                matched |= start <= byte && byte <= end;
                index += 3;
            }
            literal => {
                matched |= literal == byte;
                index += 1;
            }
        }
    }

    (false, pattern.len())
}

fn char_class_token_len(pattern: &[u8], start: usize) -> usize {
    let mut index = start + 1;
    while index < pattern.len() {
        match pattern[index] {
            b'\\' if index + 1 < pattern.len() => index += 2,
            b']' => return index - start + 1,
            _ => index += 1,
        }
    }
    pattern.len() - start
}

fn literal_prefix(pattern: &[u8]) -> CompactString {
    let mut bytes = Vec::with_capacity(pattern.len());
    let mut index = 0usize;

    while index < pattern.len() {
        match pattern[index] {
            b'*' | b'?' | b'[' => break,
            b'\\' if index + 1 < pattern.len() => {
                bytes.push(pattern[index + 1]);
                index += 2;
            }
            literal => {
                bytes.push(literal);
                index += 1;
            }
        }
    }

    CompactString::from_utf8_lossy(&bytes)
}

fn literal_segments(pattern: &[u8]) -> SmallVec<[String; 4]> {
    let mut segments = SmallVec::<[String; 4]>::new();
    let mut current = Vec::<u8>::new();
    let mut index = 0usize;

    while index < pattern.len() {
        match pattern[index] {
            b'*' | b'?' | b'[' => {
                if !current.is_empty() {
                    segments.push(String::from_utf8_lossy(&current).into_owned());
                    current.clear();
                }
                index += if pattern[index] == b'[' {
                    char_class_token_len(pattern, index)
                } else {
                    1
                };
            }
            b'\\' if index + 1 < pattern.len() => {
                current.push(pattern[index + 1]);
                index += 2;
            }
            literal => {
                current.push(literal);
                index += 1;
            }
        }
    }

    if !current.is_empty() {
        segments.push(String::from_utf8_lossy(&current).into_owned());
    }

    segments
}

fn unescape_segment(segment: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(segment.len());
    let mut index = 0usize;
    while index < segment.len() {
        if segment[index] == b'\\' && index + 1 < segment.len() {
            out.push(segment[index + 1]);
            index += 2;
        } else {
            out.push(segment[index]);
            index += 1;
        }
    }
    out
}

fn pattern_name(pattern: &[u8]) -> CompactString {
    match std::str::from_utf8(pattern) {
        Ok(pattern) => CompactString::from(pattern),
        Err(_) => CompactString::from_utf8_lossy(pattern),
    }
}

#[cfg(test)]
mod tests {
    use super::{PatternIndex, glob_match_scalar, glob_match_simd};
    use crate::{
        message::{MessageKind, PubSubMessage},
        slot::RING_SIZE,
    };
    use bytes::Bytes;
    use std::sync::Arc;

    #[test]
    fn glob_star_matches_everything() {
        assert!(glob_match_simd(b"*", b""));
        assert!(glob_match_simd(b"*", b"anything"));
    }

    #[test]
    fn glob_question_mark_matches_exactly_one_byte() {
        assert!(glob_match_simd(b"h?llo", b"hello"));
        assert!(glob_match_simd(b"h?llo", b"hallo"));
        assert!(!glob_match_simd(b"h?llo", b"hllo"));
        assert!(!glob_match_simd(b"h?llo", b"heello"));
    }

    #[test]
    fn glob_prefix_star_requires_prefix_match() {
        assert!(glob_match_simd(b"news.*", b"news.sports"));
        assert!(glob_match_simd(b"news.*", b"news.tech"));
        assert!(!glob_match_simd(b"news.*", b"xnews.sports"));
    }

    #[test]
    fn glob_character_classes_and_ranges_match() {
        assert!(glob_match_simd(b"[abc]oo", b"aoo"));
        assert!(glob_match_simd(b"[abc]oo", b"boo"));
        assert!(glob_match_simd(b"[abc]oo", b"coo"));
        assert!(!glob_match_simd(b"[abc]oo", b"doo"));
        assert!(glob_match_simd(b"[a-z]*", b"lowercase"));
        assert!(!glob_match_simd(b"[a-z]*", b"UPPER"));
    }

    #[test]
    fn glob_escaped_wildcards_match_literally() {
        assert!(glob_match_simd(br"price\*", b"price*"));
        assert!(!glob_match_simd(br"price\*", b"price9"));
    }

    #[test]
    fn simd_glob_matches_scalar_for_ten_thousand_generated_patterns() {
        let mut seed = 0x1234_5678_9abc_def0u64;
        for _ in 0..10_000 {
            let pattern = generate_pattern(&mut seed);
            let text = generate_text(&mut seed);
            assert_eq!(
                glob_match_simd(pattern.as_slice(), text.as_slice()),
                glob_match_scalar(pattern.as_slice(), text.as_slice()),
                "pattern={:?} text={:?}",
                String::from_utf8_lossy(&pattern),
                String::from_utf8_lossy(&text),
            );
        }
    }

    #[test]
    fn aho_path_notifies_all_matching_patterns() {
        let mut index = PatternIndex::default();
        let mut matches = Vec::new();
        for i in 0..94u64 {
            let _ = index.subscribe(format!("room:{i}.*").as_bytes(), i);
        }

        matches.push((b"news.*".to_vec(), index.subscribe(b"news.*", 100)));
        matches.push((b"*.sports".to_vec(), index.subscribe(b"*.sports", 101)));
        matches.push((
            b"news.sports".to_vec(),
            index.subscribe(b"news.sports", 102),
        ));
        matches.push((b"n*".to_vec(), index.subscribe(b"n*", 103)));
        matches.push((b"*sports".to_vec(), index.subscribe(b"*sports", 104)));
        matches.push((b"*".to_vec(), index.subscribe(b"*", 105)));

        let msg = Arc::new(PubSubMessage {
            channel: "news.sports".into(),
            payload: Bytes::from_static(b"payload"),
            kind: MessageKind::Message,
        });

        let delivered = index.publish_to_patterns(b"news.sports", msg);
        assert_eq!(delivered, matches.len() as u64);

        for (pattern, slot) in matches {
            let message = slot.recv().expect("pattern message");
            assert_eq!(message.channel, "news.sports");
            assert_eq!(
                message.kind,
                MessageKind::PMessage {
                    pattern: String::from_utf8(pattern).expect("pattern").into(),
                }
            );
        }
    }

    #[test]
    fn aho_path_returns_zero_for_non_matching_channels() {
        let mut index = PatternIndex::default();
        for i in 0..100u64 {
            let _ = index.subscribe(format!("room:{i}.*").as_bytes(), i);
        }

        let msg = Arc::new(PubSubMessage {
            channel: "news.sports".into(),
            payload: Bytes::from_static(b"payload"),
            kind: MessageKind::Message,
        });
        assert_eq!(index.publish_to_patterns(b"news.sports", msg), 0);
    }

    #[test]
    fn rebuild_after_subscribe_matches_immediately() {
        let mut index = PatternIndex::default();
        for i in 0..100u64 {
            let _ = index.subscribe(format!("room:{i}.*").as_bytes(), i);
        }

        let slot = index.subscribe(b"news.*", 999);
        let msg = Arc::new(PubSubMessage {
            channel: "news.world".into(),
            payload: Bytes::from_static(b"payload"),
            kind: MessageKind::Message,
        });
        assert_eq!(index.publish_to_patterns(b"news.world", msg), 1);
        assert!(slot.recv().is_some());
    }

    #[test]
    fn lagged_pattern_subscriber_is_removed_and_index_remains_usable() {
        let mut index = PatternIndex::default();
        let slot = index.subscribe(b"news.*", 1);

        for _ in 0..RING_SIZE {
            let msg = Arc::new(PubSubMessage {
                channel: "news.world".into(),
                payload: Bytes::from_static(b"x"),
                kind: MessageKind::PMessage {
                    pattern: "news.*".into(),
                },
            });
            assert!(slot.publish(msg).is_ok());
        }

        let msg = Arc::new(PubSubMessage {
            channel: "news.world".into(),
            payload: Bytes::from_static(b"payload"),
            kind: MessageKind::Message,
        });
        assert_eq!(index.publish_to_patterns(b"news.world", msg), 0);
        assert!(index.is_empty());

        let replacement = index.subscribe(b"news.*", 2);
        let msg = Arc::new(PubSubMessage {
            channel: "news.world".into(),
            payload: Bytes::from_static(b"payload"),
            kind: MessageKind::Message,
        });
        assert_eq!(index.publish_to_patterns(b"news.world", msg), 1);
        assert!(replacement.recv().is_some());
    }

    fn generate_pattern(seed: &mut u64) -> Vec<u8> {
        let mut out = Vec::new();
        let len = 1 + next(seed) as usize % 12;
        for _ in 0..len {
            match next(seed) % 8 {
                0 => out.push(b'*'),
                1 => out.push(b'?'),
                2 => out.extend_from_slice(b"[a-z]"),
                3 => {
                    out.push(b'\\');
                    out.push(b'*');
                }
                _ => out.push(b'a' + (next(seed) % 26) as u8),
            }
        }
        out
    }

    fn generate_text(seed: &mut u64) -> Vec<u8> {
        let len = next(seed) as usize % 16;
        (0..len)
            .map(|_| b'a' + (next(seed) % 26) as u8)
            .collect::<Vec<_>>()
    }

    fn next(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        *seed
    }
}
