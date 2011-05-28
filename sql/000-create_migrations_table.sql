CREATE TABLE migrations (
  name        varchar(255),
  migrated_at timestamp without time zone DEFAULT now() NOT NULL,
  user        varchar(30)
);
