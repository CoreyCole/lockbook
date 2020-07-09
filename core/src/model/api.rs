use crate::model::account::Username;
use crate::model::crypto::*;
use crate::model::file_metadata::FileMetadata;
use rsa::RSAPublicKey;
use serde::{Deserialize, Serialize};

use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ChangeDocumentContentRequest {
    pub username: String,
    pub signature: SignedValue,
    pub id: Uuid,
    pub old_metadata_version: u64,
    pub new_content: EncryptedValueWithNonce,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ChangeDocumentContentResponse {
    pub new_metadata_and_content_version: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum ChangeDocumentContentError {
    InternalError,
    InvalidAuth,
    InvalidUsername,
    ExpiredAuth,
    NotPermissioned,
    UserNotFound,
    DocumentNotFound,
    EditConflict,
    DocumentDeleted,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct CreateDocumentRequest {
    pub username: String,
    pub signature: SignedValue,
    pub id: Uuid,
    pub name: String,
    pub parent: Uuid,
    pub content: EncryptedValueWithNonce,
    pub parent_access_key: FolderAccessInfo,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct CreateDocumentResponse {
    pub new_metadata_and_content_version: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum CreateDocumentError {
    InternalError,
    InvalidAuth,
    InvalidUsername,
    ExpiredAuth,
    NotPermissioned,
    UserNotFound,
    FileIdTaken,
    DocumentPathTaken,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct DeleteDocumentRequest {
    pub username: String,
    pub signature: SignedValue,
    pub id: Uuid,
    pub old_metadata_version: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct DeleteDocumentResponse {
    pub new_metadata_and_content_version: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum DeleteDocumentError {
    InternalError,
    InvalidAuth,
    InvalidUsername,
    ExpiredAuth,
    NotPermissioned,
    UserNotFound,
    DocumentNotFound,
    EditConflict,
    DocumentDeleted,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct MoveDocumentRequest {
    pub username: String,
    pub signature: SignedValue,
    pub id: Uuid,
    pub old_metadata_version: u64,
    pub new_parent: Uuid,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct MoveDocumentResponse {
    pub new_metadata_version: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum MoveDocumentError {
    InternalError,
    InvalidAuth,
    InvalidUsername,
    ExpiredAuth,
    NotPermissioned,
    UserNotFound,
    DocumentNotFound,
    EditConflict,
    DocumentDeleted,
    DocumentPathTaken,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct RenameDocumentRequest {
    pub username: String,
    pub signature: SignedValue,
    pub id: Uuid,
    pub old_metadata_version: u64,
    pub new_name: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct RenameDocumentResponse {
    pub new_metadata_version: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum RenameDocumentError {
    InternalError,
    InvalidAuth,
    InvalidUsername,
    ExpiredAuth,
    NotPermissioned,
    UserNotFound,
    DocumentNotFound,
    DocumentDeleted,
    EditConflict,
    DocumentPathTaken,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct CreateFolderRequest {
    pub username: String,
    pub signature: SignedValue,
    pub id: Uuid,
    pub name: String,
    pub parent: Uuid,
    pub parent_access_key: FolderAccessInfo,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct CreateFolderResponse {
    pub new_metadata_version: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum CreateFolderError {
    InternalError,
    InvalidAuth,
    InvalidUsername,
    ExpiredAuth,
    NotPermissioned,
    UserNotFound,
    FileIdTaken,
    FolderPathTaken,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct DeleteFolderRequest {
    pub username: String,
    pub signature: SignedValue,
    pub id: Uuid,
    pub old_metadata_version: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct DeleteFolderResponse {
    pub new_metadata_version: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum DeleteFolderError {
    InternalError,
    InvalidAuth,
    InvalidUsername,
    ExpiredAuth,
    NotPermissioned,
    UserNotFound,
    FolderNotFound,
    EditConflict,
    FolderDeleted,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct MoveFolderRequest {
    pub username: String,
    pub signature: SignedValue,
    pub id: Uuid,
    pub old_metadata_version: u64,
    pub new_parent: Uuid,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct MoveFolderResponse {
    pub new_metadata_version: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum MoveFolderError {
    InternalError,
    InvalidAuth,
    InvalidUsername,
    ExpiredAuth,
    NotPermissioned,
    UserNotFound,
    FolderNotFound,
    EditConflict,
    FolderDeleted,
    FolderPathTaken,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct RenameFolderRequest {
    pub username: String,
    pub signature: SignedValue,
    pub id: Uuid,
    pub old_metadata_version: u64,
    pub new_name: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct RenameFolderResponse {
    pub new_metadata_version: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum RenameFolderError {
    InternalError,
    InvalidAuth,
    InvalidUsername,
    ExpiredAuth,
    NotPermissioned,
    UserNotFound,
    FolderNotFound,
    FolderDeleted,
    EditConflict,
    FolderPathTaken,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct GetPublicKeyRequest {
    pub username: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct GetPublicKeyResponse {
    pub key: RSAPublicKey,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum GetPublicKeyError {
    InternalError,
    InvalidUsername,
    UserNotFound,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct GetUpdatesRequest {
    pub username: String,
    pub signature: SignedValue,
    pub since_metadata_version: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct GetUpdatesResponse {
    pub file_metadata: Vec<FileMetadata>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum GetUpdatesError {
    InternalError,
    InvalidAuth,
    ExpiredAuth,
    NotPermissioned,
    UserNotFound,
    InvalidUsername,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct NewAccountRequest {
    pub username: Username,
    pub signature: SignedValue,
    pub public_key: RSAPublicKey,
    pub folder_id: Uuid,
    pub parent_access_key: FolderAccessInfo,
    pub user_access_key: EncryptedValue,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct NewAccountResponse {
    pub folder_metadata_version: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum NewAccountError {
    InternalError,
    InvalidAuth,
    ExpiredAuth,
    UsernameTaken,
    InvalidPublicKey,
    InvalidUserAccessKey,
    InvalidUsername,
    FileIdTaken,
}