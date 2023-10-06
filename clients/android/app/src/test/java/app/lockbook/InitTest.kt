package app.lockbook

import app.lockbook.model.CoreModel
import app.lockbook.util.Config
import app.lockbook.util.InitError
import app.lockbook.util.IntermCoreResult
import kotlinx.serialization.decodeFromString
import org.junit.BeforeClass
import org.junit.Test

class InitTest {
    companion object {
        @BeforeClass
        @JvmStatic
        fun loadLib() {
            System.loadLibrary("lb_c_v1")
        }
    }

    @Test
    fun initLoggerOk() {
        CoreModel.init(Config(false, false, createRandomPath())).unwrapOk()
    }

    @Test
    fun initLoggerUnexpected() {
        CoreModel.setUpInitLoggerParser.decodeFromString<IntermCoreResult<Unit, InitError>>(app.lockbook.core.init("")).unwrapUnexpected()
    }
}
