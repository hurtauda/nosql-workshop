package nosql.workshop.batch.mongodb;

import com.mongodb.*;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.ISODateTimeFormat;

import java.io.*;
import java.time.format.DateTimeFormatter;

/**
 * Importe les 'installations' dans MongoDB.
 */
public class InstallationsImporter {

    private final DBCollection installationsCollection;

    public InstallationsImporter(DBCollection installationsCollection) {
        this.installationsCollection = installationsCollection;
    }

    public void run() {
        InputStream is = CsvToMongoDb.class.getResourceAsStream("/csv/installations.csv");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {

            reader.lines()
                    .skip(1)
                    .filter(line -> line.length() > 0)
                    .forEach(line -> installationsCollection.save(toDbObject(line)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private DBObject toDbObject(final String line) {
        String[] columns = line
                .substring(1, line.length() - 1)
                .split("\",\"");

        BasicDBList coordinates = new BasicDBList();
        coordinates.add(columns[9]);
        coordinates.add(columns[10]);
        org.elasticsearch.common.joda.time.format.DateTimeFormatter parser = ISODateTimeFormat.date();


        BasicDBObject object =
                new BasicDBObject()
                        .append("_id", columns[1])
                        .append("nom", columns[0])
                        .append("adresse", new BasicDBObject().append("numero", columns[6]).append("voie", columns[7]).append("lieuDit", columns[5]).append("codePostal", columns[4]).append("commune", columns[2]))
                        .append("location", new BasicDBObject().append("type", "Point").append("coordinates", coordinates))
                        .append("multiCommune", columns[16].equals("Non") ? false : true)
                        .append("nbPlacesParking", Integer.parseInt((columns[17] == null || columns[17].isEmpty()) ? "0" : columns[17]))
                        .append("nbPlacesParkingHandicapes", Integer.parseInt((columns[18] == null || columns[18].isEmpty()) ? "0" : columns[18]));
                //        .append("dateMiseAJourFiche", (columns[28] == null || columns[28].isEmpty()) ? "" : parser.parseDateTime(columns[28].substring(0,10)));

        return object;
    }
}
