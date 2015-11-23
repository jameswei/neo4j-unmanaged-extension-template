package com.chitu.neo4j.unmanaged;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Created by jwei on 11/18/15.
 */
@Path("/")
public class ExtendResource {
    private final GraphDatabaseService graphDB;
    private final ObjectMapper objectMapper;

    private static final RelationshipType ACTS_IN = DynamicRelationshipType.withName("ACTS_IN");
    private static final RelationshipType DIRECTED = DynamicRelationshipType.withName("DIRECTED");
    @SuppressWarnings("unused")
    private static final Label ACTOR = DynamicLabel.label("Actor");
    private static final Label DIRECTOR = DynamicLabel.label("Director");

    public ExtendResource(@Context GraphDatabaseService graphDB) {
        this.graphDB = graphDB;
        this.objectMapper = new ObjectMapper();
    }

    @GET
    @Path("/ping")
    public Response ping() {
        return Response.status(Response.Status.OK).entity("pong").build();
    }

    @GET
    @Path("/director/{name}")
    public Response getDirector(@PathParam("name") String name) throws UnsupportedEncodingException {
        final String decodedName = URLDecoder.decode(name, "utf-8");
        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                jg.writeStartObject();
                try (Transaction tx = graphDB.beginTx()) {
                    Node director = graphDB.findNode(DIRECTOR, "name", decodedName);
                    jg.writeStringField("name", director.getProperty("name").toString());
                    jg.writeStringField("birth_place", director.getProperty("birthplace").toString());
                    jg.writeStringField("bio", director.getProperty("biography").toString());
                    tx.success();
                }
                jg.writeEndObject();
                jg.flush();
                jg.close();
            }
        };
        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/director/{name}/actors")
    public Response getCollaboratedActors(@PathParam("name") String name) throws UnsupportedEncodingException {
        final String decodedName = URLDecoder.decode(name, "utf-8");
        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                jg.writeStartArray();
                try (Transaction tx = graphDB.beginTx()) {
                    Node director = graphDB.findNode(DIRECTOR, "name", decodedName);
                    for (Relationship directed : director.getRelationships(DIRECTED, Direction.OUTGOING)) {
                        Node movie = directed.getEndNode();
                        jg.writeStartObject();
                        jg.writeObjectFieldStart("movie");
                        jg.writeStringField("title", movie.getProperty("title").toString());
                        jg.writeEndObject();
                        jg.writeArrayFieldStart("actors");
                        for (Relationship acted : movie.getRelationships(ACTS_IN, Direction.INCOMING)) {
                            Node actor = acted.getStartNode();
                            jg.writeStartObject();
                            jg.writeStringField("name", actor.getProperty("name").toString());
                            jg.writeEndObject();
                        }
                        jg.writeEndArray();
                        jg.writeEndObject();
                    }
                    tx.success();
                }
                jg.writeEndArray();
                jg.flush();
                jg.close();
            }
        };
        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

}
