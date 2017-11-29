SET datestyle TO 'iso, mdy';
UPDATE so_users SET id = id WHERE id = -1;
VACUUM so_users;