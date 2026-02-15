-- Added by Sachin Monteiro on 04-Feb-2026
ALTER TABLE csds.gd_release_package_content ADD CONSTRAINT usr_uk_release_package_content UNIQUE (f_release_package, rp_content_type, rp_content_key);
ALTER TABLE csds.gd_offence_terminal_entry  ADD CONSTRAINT usr_uk_offence_terminal_entry  UNIQUE (f_offence_revision, entry_number);
