SELECT dsl.and('beer', 'wine', 'cheese');
SELECT dsl.or('beer', 'wine', 'cheese');
SELECT dsl.not('beer', 'wine', 'cheese');

SELECT jsonb_pretty(
    dsl.bool(
        must=>dsl.must('beer', 'wine', 'cheese'),
        must_not=>dsl.must_not('beer', 'wine', 'cheese'),
        should=>dsl.should('beer', 'wine', 'cheese'),
        filter=>dsl.filter('beer', 'wine', 'cheese')
    )
);

SELECT jsonb_pretty(
    dsl.bool(
        must=>dsl.must('beer', 'wine', 'cheese')
    )
);
SELECT jsonb_pretty(
    dsl.bool(
        must_not=>dsl.must_not('beer', 'wine', 'cheese')
    )
);
SELECT jsonb_pretty(
    dsl.bool(
        should=>dsl.should('beer', 'wine', 'cheese')
    )
);
SELECT jsonb_pretty(
    dsl.bool(
        filter=>dsl.filter('beer', 'wine', 'cheese')
    )
);
