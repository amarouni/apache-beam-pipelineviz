package fr.marouni.beam.pipelineviz;

import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.attribute.RankDir;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.Graph;
import guru.nidi.graphviz.model.Node;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static guru.nidi.graphviz.model.Factory.graph;
import static guru.nidi.graphviz.model.Factory.node;
import static guru.nidi.graphviz.model.Factory.to;

/**
 * Use Apache Beam Pipeline visitor to generate a Graphviz representation of
 * the Beam pipeline
 */
public class GraphVizVisitor extends Pipeline.PipelineVisitor.Defaults {

    private Graph graph;
    private String graphOutputPath;
    private Set<BeamNode> beamNodeList;

    /**
     *
     * @param pipeline Beam pipeline
     * @param graphOutputPath Path to generated PNG output
     */
    public GraphVizVisitor(Pipeline pipeline, String graphOutputPath){
        graph = graph(pipeline.toString()).strict()
                .graphAttr().with(RankDir.RIGHT_TO_LEFT);
        this.graphOutputPath = graphOutputPath;
        this.beamNodeList = new LinkedHashSet<>();
        pipeline.traverseTopologically(this);
    }

    /**
     * Represents a Beam transform with a list of its direct parents
     */
    static class BeamNode{
        String name;
        List<String> parents;

        BeamNode(String name, List<String> parents) {
            this.name = name;
            this.parents = parents;
        }

        @Override
        public String toString() {
            return "BeamNode{" +
                    "name='" + name + '\'' +
                    ", parents=" + parents +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BeamNode beamNode = (BeamNode) o;
            return Objects.equals(name, beamNode.name) &&
                    Objects.equals(parents, beamNode.parents);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, parents);
        }
    }

    @Override
    public void leaveCompositeTransform(TransformHierarchy.Node node) {

        List<String> parents = node.getInputs().values().stream()
                .map(pValue -> getTransformName(pValue.getName()))
                .collect(Collectors.toList());
        beamNodeList.add(new BeamNode(getTransformName(node.getFullName()), parents));
    }

    public void writeGraph() throws IOException {
        Map<String, Node> tempMappings = new LinkedHashMap<>();

        for(BeamNode beamNode : beamNodeList){

            Node node = getNode(beamNode);
            tempMappings.put(beamNode.name, node);

            for(String parent : beamNode.parents){
                graph = graph
                        .with(node.link(to(tempMappings.get(parent))));
            }
        }

        Graphviz.fromGraph(graph).render(Format.PNG).toFile(new File(graphOutputPath));
    }

    /**
     * Create nidi Node from BeamNode
     */
    private Node getNode(BeamNode beamNode){
        if(beamNode.parents.isEmpty()){
            // The node is a source one
            return node(beamNode.name).with(Color.GREEN);
        } else {
            return node(beamNode.name);
        }
    }

    /**
     * Get a simplified name from Beam Internal transform name
     * @param beamInternalName ".../.../..."
     * @return Name
     */
    private String getTransformName(String beamInternalName) {
        return beamInternalName.split("/")[0];
    }
}
