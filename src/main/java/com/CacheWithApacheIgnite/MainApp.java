package com.CacheWithApacheIgnite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;

import javax.cache.Cache;
import java.util.Iterator;

public class MainApp {

    /**
     * iterate over the igniteCache
     * @param igniteCache
     */
    public static void printCache(IgniteCache igniteCache) {
        Iterator<Cache.Entry<Integer, Person>> cacheIterator = igniteCache.iterator();
        while (cacheIterator.hasNext()) {
            System.out.println(cacheIterator.next().getValue());
        }
    }

    public static void main(String[] args) {
         /* To solve that:
              SEVERE: Failed to resolve default logging config file: config/java.util.logging.properties
        */
        System.setProperty("java.util.logging.config.file", "java.util.logging.properties");

        // get the ignite config path
        String igniteCacheConfFilePath = "ignite-cache.xml";

        // start the ignite node with the config path
        Ignite ignite = IgniteFactory.createIgniteNodeWithSpecificConfiguration(
                "s", igniteCacheConfFilePath);

        // create personCache if does not exists, otherwise get the personCache
        try {
            final IgniteCache<Integer, Person> igniteCache =
                    ignite.getOrCreateCache("personCache");

            // create a new person
            Person person = new Person(5, "MehmetOzanGuven", 23, 9999);

            // load all the datas in the database to the igniteCache
            igniteCache.loadCache(null);

            // put the new person to the cache and also underlying database
            igniteCache.put(person.getPersonID(), person);

            // print the ignite cache
            printCache(igniteCache);

        } catch (IgniteException e) {
            e.printStackTrace();
        }
    }
}
