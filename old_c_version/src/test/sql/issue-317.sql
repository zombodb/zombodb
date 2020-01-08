SELECT dsl.and('beer', 'wine', 'cheese');
SELECT dsl.or('beer', 'wine', 'cheese');
SELECT dsl.not('beer', 'wine', 'cheese');

SELECT jsonb_pretty(
    dsl.bool(
        dsl.must('beer', 'wine', 'cheese'),
        dsl.must_not('beer', 'wine', 'cheese'),
        dsl.should('beer', 'wine', 'cheese'),
        dsl.filter('beer', 'wine', 'cheese')
    )
);

SELECT jsonb_pretty(
    dsl.bool(
        dsl.must('beer', 'wine', 'cheese')
    )
);
SELECT jsonb_pretty(
    dsl.bool(
        dsl.must_not('beer', 'wine', 'cheese')
    )
);
SELECT jsonb_pretty(
    dsl.bool(
        dsl.should('beer', 'wine', 'cheese')
    )
);
SELECT jsonb_pretty(
    dsl.bool(
        dsl.filter('beer', 'wine', 'cheese')
    )
);

SELECT jsonb_pretty(
    dsl.bool(
        dsl.must('beer'),
        dsl.must('wine'),
        dsl.must('cheese')
    )
);

SELECT jsonb_pretty(
    dsl.bool(
        dsl.must_not('beer'),
        dsl.must_not('wine'),
        dsl.must_not('cheese')
    )
);

SELECT jsonb_pretty(
    dsl.bool(
        dsl.should('beer'),
        dsl.should('wine'),
        dsl.should('cheese')
    )
);

SELECT jsonb_pretty(
    dsl.bool(
        dsl.filter('beer'),
        dsl.filter('wine'),
        dsl.filter('cheese')
    )
);
