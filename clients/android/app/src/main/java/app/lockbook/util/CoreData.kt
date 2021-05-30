package app.lockbook.util

import com.beust.klaxon.Json
import java.util.LinkedHashMap

data class FileMetadata(
    val id: String = "",
    @Json(name = "file_type")
    val fileType: FileType = FileType.Document,
    val parent: String = "",
    val name: String = "",
    val owner: String = "",
    @Json(name = "metadata_version")
    val metadataVersion: Long = 0,
    @Json(name = "content_version")
    val contentVersion: Long = 0,
    val deleted: Boolean = false,
    @Json(name = "user_access_keys")
    val userAccessKeys: LinkedHashMap<String, UserAccessInfo> = linkedMapOf(),
    @Json(name = "folder_access_keys")
    val folderAccessKeys: FolderAccessInfo = FolderAccessInfo()
)

data class FileUsage(
    @Json(name = "file_id")
    val fileId: String,
    @Json(name = "byte_secs")
    val byteSections: Int,
    val secs: Int,
)

data class FolderAccessInfo(
    @Json(name = "folder_id")
    val folderId: String = "",
    @Json(name = "access_key")
    val accessKey: AESEncrypted = AESEncrypted()
)

data class AESEncrypted(
    val value: List<Int> = listOf(),
    val nonce: List<Int> = listOf()
)

enum class FileType {
    Document, Folder
}

data class UserAccessInfo(
    val username: String,
    @Json(name = "encrypted_by")
    val encryptedBy: String,
    @Json(name = "access_key")
    val accessKey: AESEncrypted
)

data class Account(
    val username: String,
    @Json(name = "api_url")
    val apiUrl: String,
    @Json(name = "private_key")
    val privateKey: List<Byte>,
)

data class WorkCalculated(
    @Json(name = "work_units")
    val workUnits: List<WorkUnit>,
    @Json(name = "most_recent_update_from_server")
    val mostRecentUpdateFromServer: Long
)

data class WorkUnit(
    val tag: String,
    val content: WorkUnitMetadata
)

data class WorkUnitMetadata(val metadata: FileMetadata)

data class Config(val writeable_path: String)

enum class State {
    ReadyToUse,
    Empty,
    MigrationRequired,
    StateRequiresClearing
}

data class LocalAndServerUsages(
    @Json(name = "server_usage")
    val serverUsage: String,
    @Json(name = "uncompressed_usage")
    val uncompressedUsage: String,
    @Json(name = "data_cap")
    val dataCap: String,
)