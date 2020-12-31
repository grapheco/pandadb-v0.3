package org.neo4j.driver;

import cn.pandadb.driver.PandaDriver;
import cn.pandadb.driver.utils.PandaAuthToken;
import cn.pandadb.driver.utils.PandaDriverFactory;
import cn.pandadb.driver.utils.PandaDriverConfig;

public class GraphDatabase
{

    public static Driver driver( String uri, PandaAuthToken pandaAuthToken)
    {
        return driver( uri, pandaAuthToken, PandaDriverConfig.defaultConfiguration());
    }


    public static PandaDriver driver(String uri, PandaAuthToken pandaAuthToken, PandaDriverConfig config )
    {
        return new PandaDriverFactory(uri, pandaAuthToken, config).newInstance();
    }

}
