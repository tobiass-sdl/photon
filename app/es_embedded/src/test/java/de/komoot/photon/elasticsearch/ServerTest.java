package de.komoot.photon.elasticsearch;

import de.komoot.photon.DatabaseProperties;
import de.komoot.photon.ESBaseTester;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class ServerTest extends ESBaseTester {

    @Test
    void testSaveAndLoadFromDatabase() throws IOException {
        setUpES();

        DatabaseProperties prop = new DatabaseProperties();
        prop.setLanguages(new String[]{"en", "de", "fr"});
        Date now = new Date();
        prop.setImportDate(now);
        prop.setSupportStructuredQueries(true);
        getServer().saveToDatabase(prop);

        prop = new DatabaseProperties();
        getServer().loadFromDatabase(prop);

        assertArrayEquals(new String[]{"en", "de", "fr"}, prop.getLanguages());
        assertEquals(now, prop.getImportDate());
        assertTrue(prop.getSupportStructuredQueries());
    }
}