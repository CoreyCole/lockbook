# Data Model
## Backend
The backend tracks all data for all users. This includes user information and file metadata on a PostgreSQL database we call `IndexDB` and file contents on a DigitalOcean S3 bucket we call `FilesDB`. The server uses IndexDB to help clients sync file metadata and know when to sync file contents. The server uses FilesDB to store file contents only. Clients access FilesDB directly to read files (no authentication required because they're encrypted) but writes go through the server, which verifies users' permissions first.

As an S3 datastore, FilesDB is essentially a map from `file_id` to `file_content`. Each Lockbook file is an S3 file with `file_id` as the file name (no extension) and base64-encoded encrypted `file_content` as the only file contents.

IndexDB contains 3 tables. `users` stores users' usernames and public keys. `files` stores file names and versions. `permissions` stores which users have access to which files.

`users`:
* `username`: unique SHA1-hashed username; used for users locate each other to share content
* `pub_key_n`: modulus of user's RSA public key; used to authenticate users
* `pub_key_e`: exponent of user's RSA public key; used to authenticate users

`files`:
* `file_id`: UUID used to identify files (generated by client)
* `file_name`: base64-encoded encrypted file name; used as user-agnostic human-readable display name
* `file_content_version`: unix timestamp of last file content update (generated by database); used to detect edit conflicts
* `deleted`: whether the file is deleted; used to sync deletions across devices

`permissions`:
* `username`: SHA1-hashed username; indicates permissioned user
* `file_path`: base64-encoded encrypted file path; lets users organize files independently
* `file_id`: UUID (generated by client); indicates permissioned file
* `file_metadata_version`: unix timestamp of last metadata update (generated by database); used to determine which files a client needs to update
* `permission_type`: type of permission, currently always `owner` but will eventually include `read` and `write`

## Clients
Clients track file data for a single user. This includes the file metadata `file_id`, `file_path`, `file_name`, `file_content_version`, `file_metadata_version`, and `deleted`, as well as the file contents themselves (if synced for offline editing). In addition, clients track a single global `last_updated_version`. Clients use versions to sync metadata and detect edit conflicts.

# Usage
### Initial Sync
To initially sync:
* Clients hit the server's `/get-updated-metadata` endpoint passing a `since_version` of 0, indicating that they want the metadata of all files (with any version since the beginning of time), and save the result.
* The metadata includes the `file_id` of each file; clients use this to fetch file contents from `FilesDB`. This can be done lazily or up-front to sync for offline editing.
* The `/get-updated-metadata` endpoint also returns an `update_version` whose value the client copies to `last_updated_version`. This should be done after files are synced because if it is done before and the client is terminated before files are synced, the client will think it has all updates through `last_updated_version`, when in fact some updates were not persisted.

### Subsequent Syncs
To sync after the initial sync:
* Clients hit the server's `/get-updated-metadata` endpoint passing a `since_version` of `last_updated_version`, indicating that they want the metadata of all files that have changed since the client last sucessfully checked for updates, and save the result.
* The metadata includes the `file_metadata_version` of each file. If this is different from the locally stored `file_metadata_version`, the file metadata has changed and will be overwritten.
* The metadata includes the `file_content_version` of each file. If this is different from the locally stored `file_content_version`, the file contents have changed and the new contents should be retrieved from `FilesDB`. If there are local changes, the user will be prompted to merge the different versions of the file (if it cannot be done automatically).

### Writing Changes
When a user makes a metadata-only change, the server responds with the metadata of the affected file, which the client saves. When the user makes a content change, the server still responds with the metadata of the affected file, but the client must also pass the `file_content_version` of the file. This version must match the server's current version in order for changes to be written. If the file has been updated by another client since this client last synced, the versions will not match, which the server will indicate. The client needs to sync, and because there are local changes to the file, the sync will prompt the user to merge the different versions of the file (if it cannot be done automatically).