use senko_core::{LexBound, ScoreBound, SenkoError};

pub fn parse_score_bound(s: &[u8]) -> Result<ScoreBound, SenkoError> {
    if s.eq_ignore_ascii_case(b"+inf") {
        return Ok(ScoreBound::PosInf);
    }
    if s.eq_ignore_ascii_case(b"-inf") {
        return Ok(ScoreBound::NegInf);
    }
    if let Some(rest) = s.strip_prefix(b"(") {
        let value = fast_float::parse::<f64, _>(rest)
            .map_err(|_| SenkoError::Protocol("ERR min or max is not a float"))?;
        return Ok(ScoreBound::Exclusive(value));
    }
    let value = fast_float::parse::<f64, _>(s)
        .map_err(|_| SenkoError::Protocol("ERR min or max is not a float"))?;
    Ok(ScoreBound::Inclusive(value))
}

pub fn parse_lex_bound(s: &[u8]) -> Result<LexBound<'_>, SenkoError> {
    match s {
        b"+" => Ok(LexBound::Max),
        b"-" => Ok(LexBound::Min),
        _ if s.starts_with(b"[") => Ok(LexBound::Inclusive(&s[1..])),
        _ if s.starts_with(b"(") => Ok(LexBound::Exclusive(&s[1..])),
        _ => Err(SenkoError::Protocol(
            "ERR min or max not valid string range item",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_score_bounds() {
        assert_eq!(parse_score_bound(b"+inf").unwrap(), ScoreBound::PosInf);
        assert_eq!(parse_score_bound(b"-inf").unwrap(), ScoreBound::NegInf);
        assert_eq!(
            parse_score_bound(b"1.5").unwrap(),
            ScoreBound::Inclusive(1.5)
        );
        assert_eq!(
            parse_score_bound(b"(2.5").unwrap(),
            ScoreBound::Exclusive(2.5)
        );
    }

    #[test]
    fn parses_lex_bounds() {
        assert!(matches!(parse_lex_bound(b"+").unwrap(), LexBound::Max));
        assert!(matches!(parse_lex_bound(b"-").unwrap(), LexBound::Min));
        assert!(matches!(
            parse_lex_bound(b"[foo").unwrap(),
            LexBound::Inclusive(b"foo")
        ));
        assert!(matches!(
            parse_lex_bound(b"(foo").unwrap(),
            LexBound::Exclusive(b"foo")
        ));
        assert!(parse_lex_bound(b"foo").is_err());
    }
}
