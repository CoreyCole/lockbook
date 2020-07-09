#[cfg(test)]
mod change_file_content_tests {
    use crate::{aes_key, aes_str, api_loc, generate_account, random_filename, rsa_key, sign};
    use lockbook_core::client::{Client, ClientImpl, Error};
    use lockbook_core::model::api::*;
    use lockbook_core::model::crypto::*;
    use lockbook_core::service::crypto_service::{AesImpl, SymmetricCryptoService};
    use uuid::Uuid;

    #[test]
    fn change_file_content() {
        // new account
        let account = generate_account();
        let folder_id = Uuid::new_v4();
        let folder_key = AesImpl::generate_key();

        assert_matches!(
            ClientImpl::new_account(
                &api_loc(),
                &account.username,
                &sign(&account),
                account.keys.to_public_key(),
                folder_id,
                FolderAccessInfo {
                    folder_id: folder_id,
                    access_key: aes_key(&folder_key, &folder_key),
                },
                rsa_key(&account.keys.to_public_key(), &folder_key)
            ),
            Ok(_)
        );

        // create document
        let doc_id = Uuid::new_v4();
        let doc_key = AesImpl::generate_key();
        let version = ClientImpl::create_document(
            &api_loc(),
            &account.username,
            &sign(&account),
            doc_id,
            &random_filename(),
            folder_id,
            aes_str(&doc_key, "doc content"),
            FolderAccessInfo {
                folder_id: folder_id,
                access_key: aes_key(&folder_key, &doc_key),
            },
        )
        .unwrap();

        // change document content
        assert_matches!(
            ClientImpl::change_document_content(
                &api_loc(),
                &account.username,
                &sign(&account),
                doc_id,
                version,
                aes_str(&doc_key, "new doc content"),
            ),
            Ok(_)
        );
    }

    #[test]
    fn change_file_content_not_found() {
        // new account
        let account = generate_account();
        let folder_id = Uuid::new_v4();
        let folder_key = AesImpl::generate_key();

        assert_matches!(
            ClientImpl::new_account(
                &api_loc(),
                &account.username,
                &sign(&account),
                account.keys.to_public_key(),
                folder_id,
                FolderAccessInfo {
                    folder_id: folder_id,
                    access_key: aes_key(&folder_key, &folder_key),
                },
                rsa_key(&account.keys.to_public_key(), &folder_key)
            ),
            Ok(_)
        );

        // change content of document we never created
        assert_matches!(
            ClientImpl::change_document_content(
                &api_loc(),
                &account.username,
                &sign(&account),
                Uuid::new_v4(),
                0,
                aes_str(&folder_key, "new doc content"),
            ),
            Err(Error::<ChangeDocumentContentError>::Api(
                ChangeDocumentContentError::DocumentNotFound
            ))
        );
    }
}