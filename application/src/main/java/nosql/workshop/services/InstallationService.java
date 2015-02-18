package nosql.workshop.services;

import com.google.inject.Inject;
import nosql.workshop.model.Installation;
import nosql.workshop.model.stats.Average;
import nosql.workshop.model.stats.CountByActivity;
import org.jongo.Aggregate;
import org.jongo.MongoCollection;

import java.lang.reflect.Array;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Service permettant de manipuler les installations sportives.
 */
public class InstallationService {

    /**
     * Nom de la collection MongoDB.
     */
    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    /**
     * Retourne une installation étant donné son numéro.
     *
     * @param numero le numéro de l'installation.
     * @return l'installation correspondante, ou <code>null</code> si non trouvée.
     */
    public Installation get(String numero) {
        return this.installations.findOne("{_id: \"" + numero + "\"}").as(Installation.class);
    }

    /**
     * Retourne la liste des installations.
     *
     * @param page     la page à retourner.
     * @param pageSize le nombre d'installations par page.
     * @return la liste des installations.
     */
    public List<Installation> list(int page, int pageSize) {
        ArrayList<Installation> installs = new ArrayList<>();
        Iterator<Installation> i = this.installations.find().skip(pageSize * (page - 1)).limit(pageSize).as(Installation.class);
        while (i.hasNext()) {
            installs.add(i.next());
        }
        return installs;
    }

    /**
     * Retourne une installation aléatoirement.
     *
     * @return une installation.
     */
    public Installation random() {
        long count = count();
        int random = new Random().nextInt((int) count);
        return list(1, (int) count).get(random);
    }

    /**
     * Retourne le nombre total d'installations.
     *
     * @return le nombre total d'installations
     */
    public long count() {
        return installations.count();
    }

    /**
     * Retourne l'installation avec le plus d'équipements.
     *
     * @return l'installation avec le plus d'équipements.
     */
    public Installation installationWithMaxEquipments() {

        return this.installations.aggregate("{$project:{equipementscount:{$size:'$equipements'}, nom: 1, equipements: 1}}")
                .and("{$sort:{'equipementscount': -1}}")
                .and("{$limit: 1}")
                .as(Installation.class)
                .get(0);
    }

    /**
     * Compte le nombre d'installations par activité.
     *
     * @return le nombre d'installations par activité.
     */
    public List<CountByActivity> countByActivity() {
        return this.installations.aggregate("{$unwind: '$equipements'}")
                .and("{$unwind: '$equipements.activites'}")
                .and("{$group: {_id: '$equipements.activites', total: {$sum: 1}}}")
                .and("{$project: {_id: 0, activite: '$_id', total: 1}}")
                .as(CountByActivity.class);
    }

    public double averageEquipmentsPerInstallation() {
        return this.installations.aggregate("{$group: {_id: null, average : {$avg: {$size:'$equipements'}}}}")
                .and("{$project: {_id: 0, average: 1}}")
                .as(Average.class)
                .get(0)
                .getAverage();
    }

    /**
     * Recherche des installations sportives.
     *
     * @param searchQuery la requête de recherche.
     * @return les résultats correspondant à la requête.
     */
    public List<Installation> search(String searchQuery) {
        Iterable<Installation> iterable = installations.find("{$text: {$search: #}}", searchQuery).as(Installation.class);
        List<Installation> installs = new ArrayList<>();
        iterable.forEach(installs::add);
        return installs;
    }

    /**
     * Recherche des installations sportives par proximité géographique.
     *
     * @param lat      latitude du point de départ.
     * @param lng      longitude du point de départ.
     * @param distance rayon de recherche.
     * @return les installations dans la zone géographique demandée.
     */
    public List<Installation> geosearch(double lat, double lng, double distance) {
        this.installations.ensureIndex("{'location': '2dsphere'}");
        ArrayList<Installation> installs = new ArrayList<>();
        Iterator<Installation> i = this.installations.find("{'location': {$near: {$geometry: {type: 'Point', coordinates : [" + lng + "," + lat + "]}, $maxDistance: " + distance + "}}}").as(Installation.class);
        while (i.hasNext()) {
            installs.add(i.next());
        }
        return installs;
    }
}
