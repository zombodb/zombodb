CREATE TABLE contact (
    contact_id bigint,
    email character varying,
    telephone character varying
);
CREATE UNIQUE INDEX idxcontact_id ON contact(contact_id);

CREATE TABLE contact_person (
    personid bigint,
    firstname character varying,
    lastname character varying,
    contact_id bigint,
    foreign key (contact_id) references contact(contact_id)
);

INSERT INTO contact VALUES (1, 'cia@cia.com', 'n/a');
INSERT INTO contact_person VALUES (1, 'John', 'Hunt', 1);

CREATE INDEX idxcontact ON contact USING zombodb ((contact.*));
CREATE INDEX idxcontact_person ON contact_person USING zombodb ((contact_person.*));

SET enable_nestloop TO OFF; SET enable_bitmapscan TO OFF; SELECT cp.*, c.* FROM contact_person cp LEFT JOIN contact c ON cp.contact_id = c.contact_id WHERE c ==> '*cia*' OR cp ==> 'Hunt';

SELECT
  cp.*,
  c.*
FROM contact_person cp LEFT JOIN contact c ON cp.contact_id = c.contact_id
WHERE c ==> '*cia*' OR cp ==> 'Hunt';

DROP TABLE contact_person CASCADE;
DROP TABLE contact CASCADE;

select e.id, u.id from events e left join users u on e.user_id = u.id where e ==> 'beer' or u ==> 'vicjoecs' order by 1, 2;
select e.id, u.id from events e right join users u on e.user_id = u.id where e ==> 'beer' or u ==> 'vicjoecs' order by 1, 2;
select e.id, u.id from events e inner join users u on e.user_id = u.id where e ==> 'beer' or u ==> 'vicjoecs' order by 1, 2;

