INSERT INTO types.all_types (c_text) SELECT REPEAT('x', 65536);
INSERT INTO biz2.events (id, kind, payload) VALUES (999999, 'big-id', 'explicit-id-row') ON DUPLICATE KEY UPDATE payload='explicit-id-row';
INSERT INTO biz2.events (kind, payload) VALUES ('after-bigid', 'still-works');
