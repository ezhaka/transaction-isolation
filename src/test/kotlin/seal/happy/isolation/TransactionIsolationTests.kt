package seal.happy.isolation

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.time.Duration
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread

class TransactionIsolationTests {
    private val aliceToBob = LinkedBlockingDeque<Unit>(1)
    private val bobToAlice = LinkedBlockingDeque<Unit>(1)

    @BeforeEach
    fun setup() {
        connect()

        transaction {
            KeyValue.deleteAll()
        }
    }

    @Test
    fun `dirty read, read uncommitted`() {
        assertTrue(tryPerformDirtyRead(Connection.TRANSACTION_READ_UNCOMMITTED))
    }

    @Test
    fun `dirty read, read committed`() {
        assertFalse(tryPerformDirtyRead(Connection.TRANSACTION_READ_COMMITTED))
    }

    @Test
    fun `non-repeatable read, read committed`() {
        assertTrue(tryPerformNonRepeatableRead(Connection.TRANSACTION_READ_COMMITTED))
    }

    @Test
    fun `non-repeatable read, repeatable read`() {
        assertFalse(tryPerformNonRepeatableRead(Connection.TRANSACTION_REPEATABLE_READ))
    }

    @Test
    fun `write skew, repeatable read`() {
        assertTrue(tryPerformWriteSkew(Connection.TRANSACTION_REPEATABLE_READ))
    }

    @Test
    @Disabled
    fun `write skew, serializable`() {
        assertTimeoutPreemptively(Duration.ofSeconds(5)) {
            tryPerformWriteSkew(Connection.TRANSACTION_SERIALIZABLE)
        }
    }

    @Test
    fun `concurrent increment, read uncommitted`() {
        val result = performConcurrentIncrement(Connection.TRANSACTION_READ_UNCOMMITTED, iterations = 1000)
        assertNotEquals(1000, result)
    }

    @Test
    fun `concurrent increment, read committed`() {
        val result = performConcurrentIncrement(Connection.TRANSACTION_READ_COMMITTED, iterations = 1000)
        assertNotEquals(1000, result)
    }

    @Test
    fun `concurrent increment, repeatable read`() {
        val result = performConcurrentIncrement(Connection.TRANSACTION_REPEATABLE_READ, iterations = 1000)
        assertNotEquals(1000, result)
    }

    @Test
    fun `concurrent increment, serializable`() {
        val result = performConcurrentIncrement(Connection.TRANSACTION_SERIALIZABLE, iterations = 1000)
        assertEquals(1000, result)
    }

    private fun tryPerformDirtyRead(transactionIsolation: Int): Boolean {
        val pinkValue = AtomicInteger()
        val committedValue = 1
        val uncommittedValue = 2

        insert("pink", committedValue)

        val aliceThread = thread {
            transaction(transactionIsolation) {
                KeyValue.update(where = { KeyValue.key eq "pink" }) {
                    it[value] = uncommittedValue
                }

                aliceToBob.add(Unit)
                bobToAlice.take()
            }
        }

        val bobThread = thread {
            transaction(transactionIsolation) {
                aliceToBob.take()
                pinkValue.set(KeyValue.select { KeyValue.key eq "pink" }.first()[KeyValue.value])
                bobToAlice.add(Unit)
            }
        }

        aliceThread.join()
        bobThread.join()

        return pinkValue.get() == uncommittedValue
    }

    private fun tryPerformNonRepeatableRead(transactionIsolation: Int): Boolean {
        insert("teal", 1)

        val aliceThread = thread {
            transaction(transactionIsolation) {
                bobToAlice.take()

                KeyValue.update(where = { KeyValue.key eq "teal" }) {
                    it[value] = 2
                }
            }

            aliceToBob.add(Unit)
        }

        val firstReadValue = AtomicInteger()
        val secondReadValue = AtomicInteger()

        val bobThread = thread {
            transaction(transactionIsolation) {
                firstReadValue.set(KeyValue.select { KeyValue.key eq "teal" }.first()[KeyValue.value])

                bobToAlice.add(Unit)
                aliceToBob.take()

                secondReadValue.set(KeyValue.select { KeyValue.key eq "teal" }.first()[KeyValue.value])
            }
        }

        aliceThread.join()
        bobThread.join()

        return firstReadValue.get() != secondReadValue.get()
    }

    private fun tryPerformWriteSkew(transactionIsolation: Int): Boolean {
        insert("orange", 0)

        val aliceThread = thread {
            transaction(transactionIsolation) {
                bobToAlice.take()

                val count = KeyValue.select { KeyValue.key eq "orange" }.first()[KeyValue.value]
                KeyValue.update(where = { KeyValue.key eq "orange" }) { it[value] = count + 1 }
            }

            aliceToBob.add(Unit)
        }

        val bobThread = thread {
            transaction(transactionIsolation) {
                val count = KeyValue.select { KeyValue.key eq "orange" }.first()[KeyValue.value]

                bobToAlice.add(Unit)
                aliceToBob.take()

                KeyValue.update(where = { KeyValue.key eq "orange" }) { it[value] = count + 1 }
            }
        }

        aliceThread.join()
        bobThread.join()

        val count = transaction { KeyValue.select { KeyValue.key eq "orange" }.count() }
        return count == 1
    }

    private fun performConcurrentIncrement(transactionIsolation: Int, iterations: Int = 1000): Int {
        insert("orange", 0)

        val pool = Executors.newFixedThreadPool(Math.max(Runtime.getRuntime().availableProcessors(), 8))
        val completedTransactions = AtomicInteger()

        for (i in 1..iterations) {
            pool.submit {
                transaction(transactionIsolation, repetitionAttempts = iterations) {
                    val count = KeyValue.select { KeyValue.key eq "orange" }.first()[KeyValue.value]
                    KeyValue.update(where = { KeyValue.key eq "orange" }) { it[value] = count + 1 }
                }

                completedTransactions.incrementAndGet()
            }
        }

        pool.shutdown()
        pool.awaitTermination(1, TimeUnit.MINUTES)

        assertEquals(iterations, completedTransactions.get())

        return transaction(transactionIsolation) {
            KeyValue.select { KeyValue.key eq "orange" }.first()[KeyValue.value]
        }
    }

    private fun insert(key: String, value: Int) = transaction {
        KeyValue.insert {
            it[this.key] = key
            it[this.value] = value
        }
    }
}
