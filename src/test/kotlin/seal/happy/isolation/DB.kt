package seal.happy.isolation

import org.jetbrains.exposed.dao.IntIdTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction

object KeyValue : IntIdTable() {
    val key = varchar("key_", 50)
    val value = integer("value").default(0)
}

fun connect() {
    Database.connect(
        "jdbc:mysql://localhost:3307/test",
        driver = "com.mysql.jdbc.Driver",
        user = "root",
        password = "test"
    )

    transaction {
        SchemaUtils.create(KeyValue)
    }
}

fun <T> transaction(transactionIsolation: Int, repetitionAttempts: Int? = null, statement: Transaction.() -> T): T =
    org.jetbrains.exposed.sql.transactions.transaction(
        transactionIsolation,
        repetitionAttempts ?: TransactionManager.manager.defaultRepetitionAttempts,
        null,
        statement
    )
