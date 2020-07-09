#[cfg(test)]
mod sync_tests {
    use crate::{random_username, test_db};
    use lockbook_core::model::crypto::DecryptedValue;
    use lockbook_core::model::work_unit::WorkUnit;
    use lockbook_core::repo::file_metadata_repo::FileMetadataRepo;
    use lockbook_core::service::account_service::AccountService;
    use lockbook_core::service::file_service::FileService;
    use lockbook_core::service::sync_service::SyncService;
    use lockbook_core::{
        init_logger_safely, DefaultAccountService, DefaultFileMetadataRepo, DefaultFileService,
        DefaultSyncService,
    };

    #[test]
    fn test_create_files_and_folders_sync() {
        let db = test_db();
        let account = DefaultAccountService::create_account(&db, &random_username()).unwrap();

        assert_eq!(
            DefaultSyncService::calculate_work(&db)
                .unwrap()
                .work_units
                .len(),
            0
        );

        DefaultFileService::create_at_path(
            &db,
            format!("{}/a/b/c/test", account.username).as_str(),
        )
        .unwrap();

        assert_eq!(
            DefaultSyncService::calculate_work(&db)
                .unwrap()
                .work_units
                .len(),
            4
        );

        assert!(DefaultSyncService::sync(&db).is_ok());

        let db2 = test_db();
        DefaultAccountService::import_account(
            &db2,
            &DefaultAccountService::export_account(&db).unwrap(),
        )
        .unwrap();

        assert_eq!(
            DefaultSyncService::calculate_work(&db2)
                .unwrap()
                .work_units
                .len(),
            5
        );

        DefaultSyncService::sync(&db2).unwrap();
        assert_eq!(
            DefaultFileMetadataRepo::get_all(&db).unwrap(),
            DefaultFileMetadataRepo::get_all(&db2).unwrap()
        );

        assert_eq!(
            DefaultSyncService::calculate_work(&db2)
                .unwrap()
                .work_units
                .len(),
            0
        );
    }

    #[test]
    fn test_edit_document_sync() {
        init_logger_safely();
        let db = test_db();
        let account = DefaultAccountService::create_account(&db, &random_username()).unwrap();
        println!("Made account");

        assert_eq!(
            DefaultSyncService::calculate_work(&db)
                .unwrap()
                .work_units
                .len(),
            0
        );
        println!("1st calculate work");

        let file = DefaultFileService::create_at_path(
            &db,
            format!("{}/a/b/c/test", account.username).as_str(),
        )
        .unwrap();

        assert!(DefaultSyncService::sync(&db).is_ok());
        println!("1st sync done");

        let db2 = test_db();
        DefaultAccountService::import_account(
            &db2,
            &DefaultAccountService::export_account(&db).unwrap(),
        )
        .unwrap();

        DefaultSyncService::sync(&db2).unwrap();
        println!("2nd sync done, db2");

        DefaultFileService::write_document(
            &db,
            file.id,
            &DecryptedValue::from("meaningful messages"),
        )
        .unwrap();

        assert_eq!(
            DefaultSyncService::calculate_work(&db)
                .unwrap()
                .work_units
                .len(),
            1
        );
        println!("2nd calculate work, db1, 1 dirty file");

        match DefaultSyncService::calculate_work(&db)
            .unwrap()
            .work_units
            .get(0)
            .unwrap()
            .clone()
        {
            WorkUnit::LocalChange { metadata } => assert_eq!(metadata.name, file.name),
            WorkUnit::ServerChange { .. } => {
                panic!("This should have been a local change with no server changes!")
            }
        };
        println!("3rd calculate work, db1, 1 dirty file");

        DefaultSyncService::sync(&db).unwrap();
        println!("3rd sync done, db1, dirty file pushed");

        assert_eq!(
            DefaultSyncService::calculate_work(&db)
                .unwrap()
                .work_units
                .len(),
            0
        );
        println!("4th calculate work, db1, dirty file pushed");

        assert_eq!(
            DefaultSyncService::calculate_work(&db2)
                .unwrap()
                .work_units
                .len(),
            1
        );
        println!("5th calculate work, db2, dirty file needs to be pulled");

        let edited_file = DefaultFileMetadataRepo::get(&db, file.id).unwrap();

        match DefaultSyncService::calculate_work(&db2)
            .unwrap()
            .work_units
            .get(0)
            .unwrap()
            .clone()
        {
            WorkUnit::ServerChange { metadata } => assert_eq!(metadata, edited_file),
            WorkUnit::LocalChange { .. } => {
                panic!("This should have been a ServerChange with no LocalChange!")
            }
        };
        println!("6th calculate work, db2, dirty file needs to be pulled");

        DefaultSyncService::sync(&db2).unwrap();
        println!("4th sync done, db2, dirty file pulled");
        assert_eq!(
            DefaultSyncService::calculate_work(&db2)
                .unwrap()
                .work_units
                .len(),
            0
        );
        println!("7th calculate work ");

        assert_eq!(
            DefaultFileService::read_document(&db2, edited_file.id)
                .unwrap()
                .secret,
            "meaningful messages".to_string()
        );
    }
}