package nosql.workshop.services;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import nosql.workshop.model.Installation;
import nosql.workshop.model.suggest.TownSuggest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;

import java.io.IOException;
import java.util.*;

/**
 * Created by Chris on 12/02/15.
 */
public class SearchService {
    public static final String INSTALLATIONS_INDEX = "installations";
    public static final String INSTALLATION_TYPE = "installation";
    public static final String TOWNS_INDEX = "towns";
    private static final String TOWN_TYPE = "town";


    public static final String ES_HOST = "es.host";
    public static final String ES_TRANSPORT_PORT = "es.transport.port";

    final Client elasticSearchClient;
    final ObjectMapper objectMapper;

    @Inject
    public SearchService(@Named(ES_HOST) String host, @Named(ES_TRANSPORT_PORT) int transportPort) {
        //TODO à retirer avant pull request
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "PSG").build();

        elasticSearchClient = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(host, transportPort));

        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Recherche les installations à l'aide d'une requête full-text
     *
     * @param searchQuery la requête
     * @return la listes de installations
     */
    public List<Installation> search(String searchQuery) {
        // TODO codez le service
        throw new UnsupportedOperationException();
    }

    /**
     * Transforme un résultat de recherche ES en objet installation.
     *
     * @param searchHit l'objet ES.
     * @return l'installation.
     */
    private Installation mapToInstallation(SearchHit searchHit) {
        try {
            return objectMapper.readValue(searchHit.getSourceAsString(), Installation.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<TownSuggest> suggestTownName(String townName) {
        CompletionSuggestionBuilder compBuilder = new CompletionSuggestionBuilder("towns");
        compBuilder.text(townName);
        compBuilder.field("townNameS");

        SearchResponse searchResponse = elasticSearchClient.prepareSearch(TOWNS_INDEX)
                .setTypes("completion")
                .setQuery(QueryBuilders.matchAllQuery())
                .addSuggestion(compBuilder)
                .execute().actionGet();

        CompletionSuggestion compSuggestion = searchResponse.getSuggest().getSuggestion("towns");
        List<TownSuggest> townSuggests =  new ArrayList<TownSuggest>();
        Iterator<CompletionSuggestion.Entry.Option> it = compSuggestion.iterator().next().getOptions().iterator();
        while (it.hasNext()){
            try {
                townSuggests.add(objectMapper.readValue(it.next().getPayloadAsString(), TownSuggest.class));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return townSuggests;

    }

    public Double[] getTownLocation(String townName) {
        SearchResponse response = elasticSearchClient.prepareSearch("towns")
                .setTypes("town")
                .setQuery(QueryBuilders.matchQuery("townName", townName))
                .execute()
                .actionGet();

        SearchHits searchHits = response.getHits();
        TownSuggest ts;
        try {
            ts = objectMapper.readValue(searchHits.getHits()[0].getSourceAsString(), TownSuggest.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return ts.getLocation();
    }
}
