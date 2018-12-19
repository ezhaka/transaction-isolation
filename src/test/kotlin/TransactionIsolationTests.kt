import org.jetbrains.exposed.dao.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

class TransactionIsolationTests {
    @BeforeEach
    fun setup() {
        Database.connect(
            "jdbc:mysql://localhost:3307/test",
            driver = "com.mysql.jdbc.Driver",
            user = "root",
            password = "test"
        )

        transaction {
            SchemaUtils.create(Colors)
            Colors.deleteAll()
        }
    }

    @Test
    fun dirtyRead_ReadUncommitted() {
        assertTrue(tryPerformDirtyRead(Connection.TRANSACTION_READ_UNCOMMITTED))
    }

    @Test
    fun dirtyRead_ReadCommitted() {
        assertFalse(tryPerformDirtyRead(Connection.TRANSACTION_READ_COMMITTED))
    }

    @Test
    fun nonRepeatableRead_ReadCommitted() {
        assertTrue(tryPerformNonRepeatableRead(Connection.TRANSACTION_READ_COMMITTED))
    }

    @Test
    fun nonRepeatableRead_RepeatableRead() {
        assertFalse(tryPerformNonRepeatableRead(Connection.TRANSACTION_REPEATABLE_READ))
    }

    private fun tryPerformDirtyRead(transactionIsolation: Int): Boolean {
        val counter = AtomicInteger()
        val aliceToBob = LinkedBlockingDeque<Unit>(1)
        val bobToAlice = LinkedBlockingDeque<Unit>(1)

        val aliceThread = thread {
            transaction(transactionIsolation) {
                Colors.insert {
                    it[name] = "pink"
                }

                aliceToBob.add(Unit)
                bobToAlice.take()
            }
        }

        val bobThread = thread {
            transaction(transactionIsolation) {
                aliceToBob.take()
                counter.set(Colors.select { Colors.name eq "pink" }.count())
                bobToAlice.add(Unit)
            }
        }

        aliceThread.join()
        bobThread.join()

        return counter.get() == 1
    }

    private fun tryPerformNonRepeatableRead(transactionIsolation: Int): Boolean {
        val aliceToBob = LinkedBlockingDeque<Unit>(1)
        val bobToAlice = LinkedBlockingDeque<Unit>(1)

        val aliceThread = thread {
            transaction(transactionIsolation) {
                bobToAlice.take()

                Colors.insert {
                    it[name] = "teal"
                }
            }

            aliceToBob.add(Unit)
        }

        val firstReadCounter = AtomicInteger()
        val secondReadCounter = AtomicInteger()

        val bobThread = thread {
            transaction(transactionIsolation) {
                bobToAlice.add(Unit)

                firstReadCounter.set(Colors.select { Colors.name eq "teal" }.count())

                aliceToBob.take()

                secondReadCounter.set(Colors.select { Colors.name eq "teal" }.count())
            }
        }

        aliceThread.join()
        bobThread.join()

        return firstReadCounter.get() != secondReadCounter.get()
    }

}

fun <T> transaction(transactionIsolation: Int, statement: Transaction.() -> T): T =
    transaction(transactionIsolation, TransactionManager.manager.defaultRepetitionAttempts, null, statement)

object Colors : IntIdTable() {
    val name = varchar("name", 50)
}