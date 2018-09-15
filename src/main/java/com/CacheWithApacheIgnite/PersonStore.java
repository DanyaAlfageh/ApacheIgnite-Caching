package com.CacheWithApacheIgnite;

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.
        TransactionOptimisticException;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.sql.*;

public class PersonStore extends CacheStoreAdapter<Integer, Person> {

    /**
     * Postgres JDBC connection method
     * For my example:
     * PERSONS table is in testdb database
     * and testdb database password is 1234
     * @return connection
     */
    private Connection connectTheDatabase(){
        Connection databaseConnection = null;

        try{
            databaseConnection = DriverManager.
                    getConnection("jdbc:postgresql:" +
                            "//localhost:5432/" +
                            "testdb", "postgres", "1234");

        }
        catch (SQLException e){
            e.printStackTrace();
        }
        return databaseConnection;
    }

    /**
     * According to the query result find the
     * correspond com.CacheWithApacheIgnite.Person object
     * @param queryResult
     * @return correspond person object
     */
    private Person getPersonFromQueryResult(ResultSet queryResult){
        Person person = null;

        try{
            while(queryResult.next()){
                int personID = queryResult.getInt(1);
                String personName = queryResult.getString(2);
                int personAge = queryResult.getInt(3);
                double personSalary = queryResult.getDouble(4);

                person = new Person(personID, personName, personAge, personSalary);

                return person;
            }
        }
        catch (SQLException e){
            e.printStackTrace();
        }

        return person;

    }


    /**
     * It is called	whenever IgniteCache.loadCache(...)	 method is called.
     * This is method is used to load datas
     * to cache from database
     * In my example, I decided to load all the
     * person in the persons table
     * @param clo
     * @param args
     */
    public void loadCache(IgniteBiInClosure<Integer,
            Person> clo, Object... args){
        System.out.println("Loading the all database to the cache");
        Connection postgresConnection = connectTheDatabase();

        while(true){
            try(Transaction transaction = Ignition.ignite().transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE)){

                PreparedStatement sqlStatement = postgresConnection.
                        prepareStatement("select * from PERSONS");
                ResultSet queryResult = sqlStatement.executeQuery();
                Person person = null;
                while(queryResult.next()){
                    int personID = queryResult.getInt(1);
                    String personName = queryResult.getString(2);
                    int personAge = queryResult.getInt(3);
                    double personSalary = queryResult.getDouble(4);

                    person = new Person(personID,
                                        personName,
                                        personAge,
                                        personSalary);
                    clo.apply(personID, person);
                }
                transaction.commit();
                break;
            }
            catch(SQLException | TransactionOptimisticException e){
                e.printStackTrace();
            }
        }
    }

    /**
     * It is called	whenever IgniteCache.get(key) method is called.
     * Key is the person id.
     * This is method is used to load single data to cache from database
     * @param key comes from  IgniteCache.get() method
     * @return com.CacheWithApacheIgnite.Person object from the database. If there is match,
     * otherwise returns null
     */
    @Override
    public Person load(Integer key) throws CacheLoaderException {
        System.out.println("New com.CacheWithApacheIgnite.Person object is loading to cache from database...");

        Connection postgresConnection = connectTheDatabase();

        while(true){
            try(Transaction transaction = Ignition.ignite().transactions().
                    txStart(TransactionConcurrency.OPTIMISTIC,
                            TransactionIsolation.SERIALIZABLE)){
                PreparedStatement sqlStatement = postgresConnection.
                        prepareStatement("select * from PERSONS where id = ?");
                sqlStatement.setInt(1, key);
                ResultSet queryResult = sqlStatement.executeQuery();
                Person person = getPersonFromQueryResult(queryResult);
                transaction.commit();
                return person;
            }
            catch(SQLException | TransactionOptimisticException e){
                e.printStackTrace();
            }
        }
    }

    /**
     * It is called	whenever IgniteCache.put(com.CacheWithApacheIgnite.Person person) method is called.
     * When cache is updated with new com.CacheWithApacheIgnite.Person,
     *      I will add the new cache data to the database.
     *      (also you can check the data in the database already, if it is not then add it)
     * @param entry is the parameter that is com.CacheWithApacheIgnite.Person object
     */
    @Override
    public void write(Cache.Entry<? extends Integer, ? extends Person> entry) throws CacheWriterException {
        Connection postgresConnection = connectTheDatabase();
        Person person = entry.getValue();
        while(true){
            try(Transaction transaction = Ignition.ignite().transactions().
                    txStart(TransactionConcurrency.OPTIMISTIC,
                            TransactionIsolation.SERIALIZABLE)){


                PreparedStatement sqlStatement = postgresConnection.
                        prepareStatement("insert into PERSONS (name, age, salary) values (?, ?, ?)");
                sqlStatement.setString(1, person.getName());
                sqlStatement.setInt(2, person.getAge());
                sqlStatement.setDouble(3, person.getSalary());
                sqlStatement.executeUpdate();

                transaction.commit();
                break;

            }
            catch (SQLException | TransactionOptimisticException e){

                e.printStackTrace();
            }
        }
    }


    /**
     * It is called	whenever IgniteCache.remove() method is	called.
     * Remove person from cache and database
     * @param key is the person id
     * @throws CacheWriterException
     */
    @Override
    public void delete(Object key) throws CacheWriterException {
        Connection postgresConnection = connectTheDatabase();

        while(true){
            try(Transaction transaction = Ignition.ignite().transactions().txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE)){
                PreparedStatement sqlStatement = postgresConnection.prepareStatement("delete from PERSONS where id = ?");
                int key_int = Integer.parseInt(String.valueOf(key));
                sqlStatement.setInt(1, key_int);
                sqlStatement.executeUpdate();
                transaction.commit();
                break;

            }
            catch (SQLException | TransactionOptimisticException e){
                e.printStackTrace();
            }
        }

    }
}
