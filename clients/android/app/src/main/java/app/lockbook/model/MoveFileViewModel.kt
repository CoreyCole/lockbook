package app.lockbook.model

import androidx.lifecycle.LiveData
import androidx.lifecycle.MutableLiveData
import androidx.lifecycle.ViewModel
import app.lockbook.util.*
import com.github.michaelbull.result.Err
import com.github.michaelbull.result.Ok
import kotlinx.coroutines.*

class MoveFileViewModel(path: String) :
    ViewModel(),
    RegularClickInterface {

    private var job = Job()
    private val uiScope = CoroutineScope(Dispatchers.Main + job)
    private val config = Config(path)
    lateinit var currentParent: FileMetadata
    lateinit var ids: Array<String>
    lateinit var names: Array<String>

    private val _files = MutableLiveData<List<FileMetadata>>()
    private val _closeDialog = MutableLiveData<Unit>()
    private val _errorHasOccurred = SingleMutableLiveData<String>()
    private val _unexpectedErrorHasOccurred = SingleMutableLiveData<String>()

    val files: LiveData<List<FileMetadata>>
        get() = _files

    val closeDialog: LiveData<Unit>
        get() = _closeDialog

    val errorHasOccurred: LiveData<String>
        get() = _errorHasOccurred

    val unexpectedErrorHasOccurred: LiveData<String>
        get() = _unexpectedErrorHasOccurred

    init {
        uiScope.launch {
            withContext(Dispatchers.IO) {
                startInRoot()
            }
        }
    }

    fun startInRoot() {
        uiScope.launch {
            withContext(Dispatchers.IO) {
                when (val rootResult = CoreModel.getRoot(config)) {
                    is Ok -> {
                        currentParent = rootResult.value
                        refreshOverFolder()
                    }
                    is Err -> when (val error = rootResult.error) {
                        is GetRootError.NoRoot -> _errorHasOccurred.postValue("Error! No root!")
                        is GetRootError.Unexpected -> _unexpectedErrorHasOccurred.postValue(error.error)
                    }
                }.exhaustive
            }
        }
    }

    fun moveFilesToFolder() {
        uiScope.launch {
            withContext(Dispatchers.IO) {
                var hasErrorOccurred = false
                for (id in ids) {
                    val moveFileResult = moveFileIfSuccessful(id)
                    if (!moveFileResult) {
                        hasErrorOccurred = !moveFileResult
                        break
                    }
                }

                if (!hasErrorOccurred) {
                    _closeDialog.postValue(Unit)
                }
            }
        }
    }

    private fun moveFileIfSuccessful(id: String): Boolean {
        return when (val moveFileResult = CoreModel.moveFile(config, id, currentParent.id)) {
            is Ok -> true
            is Err -> {
                val fileName = names[ids.indexOf(id)]
                when (val error = moveFileResult.error) {
                    MoveFileError.NoAccount -> _errorHasOccurred.postValue("Error! $fileName has no account!")
                    MoveFileError.FileDoesNotExist -> _errorHasOccurred.postValue("Error! $fileName does not exist!")
                    MoveFileError.DocumentTreatedAsFolder -> _errorHasOccurred.postValue("Error! $fileName treated as folder!")
                    MoveFileError.TargetParentDoesNotExist -> _errorHasOccurred.postValue("Error! The parent file does not exist!")
                    MoveFileError.TargetParentHasChildNamedThat -> _errorHasOccurred.postValue("Error! The parent file has a child named $fileName already!")
                    MoveFileError.CannotMoveRoot -> _errorHasOccurred.postValue("Error! You cannot move root!")
                    is MoveFileError.Unexpected -> _unexpectedErrorHasOccurred.postValue(error.error)
                }
                false
            }
        }.exhaustive
    }

    private fun refreshOverFolder() {
        when (val getChildrenResult = CoreModel.getChildren(config, currentParent.id)) {
            is Ok -> {
                val tempFiles = getChildrenResult.value.filter { fileMetadata -> fileMetadata.fileType == FileType.Folder && !ids.contains(fileMetadata.id) }.toMutableList()
                tempFiles.add(0, FileMetadata(name = "..", parent = "The parent file is ${currentParent.name}"))
                _files.postValue(tempFiles)
            }
            is Err -> when (val error = getChildrenResult.error) {
                is GetChildrenError.Unexpected -> {
                    _unexpectedErrorHasOccurred.postValue(error.error)
                }
            }
        }.exhaustive
    }

    private fun setParentAsParent() {
        when (val getFileById = CoreModel.getFileById(config, currentParent.parent)) {
            is Ok -> currentParent = getFileById.value
            is Err -> when (val error = getFileById.error) {
                GetFileByIdError.NoFileWithThatId -> _errorHasOccurred.postValue("Error! No file with that id!")
                is GetFileByIdError.Unexpected -> _unexpectedErrorHasOccurred.postValue(error.error)
            }
        }.exhaustive
    }

    override fun onItemClick(position: Int) {
        uiScope.launch {
            withContext(Dispatchers.IO) {
                _files.value?.let { files ->
                    if (position == 0) {
                        setParentAsParent()
                        refreshOverFolder()
                    } else {
                        currentParent = files[position]
                        refreshOverFolder()
                    }
                }
            }
        }
    }
}