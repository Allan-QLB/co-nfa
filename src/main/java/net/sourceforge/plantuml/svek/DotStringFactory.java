/* ========================================================================
 * PlantUML : a free UML diagram generator
 * ========================================================================
 *
 * (C) Copyright 2009-2017, Arnaud Roques
 *
 * Project Info:  http://plantuml.com
 *
 * If you like this project or if you find it useful, you can support us at:
 *
 * http://plantuml.com/patreon (only 1$ per month!)
 * http://plantuml.com/paypal
 *
 * This file is part of PlantUML.
 *
 * PlantUML is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * PlantUML distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public
 * License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 *
 *
 * Original Author:  Arnaud Roques
 *
 *
 */
package net.sourceforge.plantuml.svek;

import net.sourceforge.plantuml.BaseFile;
import net.sourceforge.plantuml.ISkinParam;
import net.sourceforge.plantuml.StringUtils;
import net.sourceforge.plantuml.UmlDiagramType;
import net.sourceforge.plantuml.cucadiagram.CucaDiagram;
import net.sourceforge.plantuml.cucadiagram.IGroup;
import net.sourceforge.plantuml.cucadiagram.Rankdir;
import net.sourceforge.plantuml.cucadiagram.dot.*;
import net.sourceforge.plantuml.graphic.StringBounder;
import net.sourceforge.plantuml.graphic.TextBlock;
import net.sourceforge.plantuml.posimo.Moveable;
import net.sourceforge.plantuml.vizjs.GraphvizJs;
import net.sourceforge.plantuml.vizjs.GraphvizJsRuntimeException;

import java.awt.geom.Point2D;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DotStringFactory implements Moveable {

    private final Bibliotekon bibliotekon = new Bibliotekon();

    final private Set<String> rankMin = new HashSet<String>();

    private final ColorSequence colorSequence;
    private final Cluster root;

    private Cluster current;
    private final UmlDiagramType umlDiagramType;
    private final ISkinParam skinParam;
    private final DotMode dotMode;

    private final StringBounder stringBounder;

    public DotStringFactory(StringBounder stringBounder, DotData dotData) {
        this.skinParam = dotData.getSkinParam();
        this.umlDiagramType = dotData.getUmlDiagramType();
        this.dotMode = dotData.getDotMode();

        this.colorSequence = new ColorSequence();
        this.stringBounder = stringBounder;
        this.root = new Cluster(colorSequence, skinParam, dotData.getRootGroup());
        this.current = root;
    }

    public DotStringFactory(StringBounder stringBounder, CucaDiagram diagram) {
        this.skinParam = diagram.getSkinParam();
        this.umlDiagramType = diagram.getUmlDiagramType();
        this.dotMode = DotMode.NORMAL;

        this.colorSequence = new ColorSequence();
        this.stringBounder = stringBounder;
        this.root = new Cluster(colorSequence, skinParam, diagram.getEntityFactory().getRootGroup());
        this.current = root;
    }

    public void addShape(Shape shape) {
        current.addShape(shape);
    }

    private void printMinRanking(StringBuilder sb) {
        if (rankMin.size() == 0) {
            return;
        }
        sb.append("{ rank = min;");
        for (String id : rankMin) {
            sb.append(id);
            sb.append(";");
        }
        sb.append("}");

    }

    private double getHorizontalDzeta() {
        double max = 0;
        for (Line l : bibliotekon.allLines()) {
            final double c = l.getHorizontalDzeta(stringBounder);
            if (c > max) {
                max = c;
            }
        }
        return max / 10;
    }

    private double getVerticalDzeta() {
        double max = 0;
        for (Line l : bibliotekon.allLines()) {
            final double c = l.getVerticalDzeta(stringBounder);
            if (c > max) {
                max = c;
            }
        }
        return max / 10;
    }

    String createDotString(String... dotStrings) {
        final StringBuilder sb = new StringBuilder();

        double nodesep = getHorizontalDzeta();
        if (nodesep < getMinNodeSep()) {
            nodesep = getMinNodeSep();
        }
        if (skinParam.getNodesep() != 0) {
            nodesep = skinParam.getNodesep();
        }
        final String nodesepInches = SvekUtils.pixelToInches(nodesep);
        // Log.println("nodesep=" + nodesepInches);
        double ranksep = getVerticalDzeta();
        if (ranksep < getMinRankSep()) {
            ranksep = getMinRankSep();
        }
        if (skinParam.getRanksep() != 0) {
            ranksep = skinParam.getRanksep();
        }
        final String ranksepInches = SvekUtils.pixelToInches(ranksep);
        // Log.println("ranksep=" + ranksepInches);
        sb.append("digraph unix {");
        SvekUtils.println(sb);

        for (String s : dotStrings) {
            if (s.startsWith("ranksep")) {
                sb.append("ranksep=" + ranksepInches + ";");
            } else if (s.startsWith("nodesep")) {
                sb.append("nodesep=" + nodesepInches + ";");
            } else {
                sb.append(s);
            }
            SvekUtils.println(sb);
        }
        // sb.append("newrank=true;");
        // SvekUtils.println(sb);
        sb.append("remincross=true;");
        SvekUtils.println(sb);
        sb.append("searchsize=500;");
        SvekUtils.println(sb);
        // if (OptionFlags.USE_COMPOUND) {
        // sb.append("compound=true;");
        // SvekUtils.println(sb);
        // }

        final DotSplines dotSplines = skinParam.getDotSplines();
        if (dotSplines == DotSplines.POLYLINE) {
            sb.append("splines=polyline;");
            SvekUtils.println(sb);
        } else if (dotSplines == DotSplines.ORTHO) {
            sb.append("splines=ortho;");
            SvekUtils.println(sb);
        }

        if (skinParam.getRankdir() == Rankdir.LEFT_TO_RIGHT) {
            sb.append("rankdir=LR;");
            SvekUtils.println(sb);
        }

        manageMinMaxCluster(sb);

        root.printCluster1(sb, bibliotekon.allLines(), stringBounder);
        for (Line line : bibliotekon.lines0()) {
            line.appendLine(getGraphvizVersion(), sb, dotMode);
        }
        root.fillRankMin(rankMin);
        root.printCluster2(sb, bibliotekon.allLines(), stringBounder, dotMode, getGraphvizVersion(), umlDiagramType);
        printMinRanking(sb);

        for (Line line : bibliotekon.lines1()) {
            line.appendLine(getGraphvizVersion(), sb, dotMode);
        }
        SvekUtils.println(sb);
        sb.append("}");

        return sb.toString();
    }

    private void manageMinMaxCluster(final StringBuilder sb) {
        final List<String> minPointCluster = new ArrayList<String>();
        final List<String> maxPointCluster = new ArrayList<String>();
        for (Cluster cluster : bibliotekon.allCluster()) {
            final String minPoint = cluster.getMinPoint(umlDiagramType);
            if (minPoint != null) {
                minPointCluster.add(minPoint);
            }
            final String maxPoint = cluster.getMaxPoint(umlDiagramType);
            if (maxPoint != null) {
                maxPointCluster.add(maxPoint);
            }
        }
        if (minPointCluster.size() > 0) {
            sb.append("{rank=min;");
            for (String s : minPointCluster) {
                sb.append(s);
                sb.append(" [shape=point,width=.01,label=\"\"]");
                sb.append(";");
            }
            sb.append("}");
            SvekUtils.println(sb);
        }
        if (maxPointCluster.size() > 0) {
            sb.append("{rank=max;");
            for (String s : maxPointCluster) {
                sb.append(s);
                sb.append(" [shape=point,width=.01,label=\"\"]");
                sb.append(";");
            }
            sb.append("}");
            SvekUtils.println(sb);
        }
    }

    private int getMinRankSep() {
        if (umlDiagramType == UmlDiagramType.ACTIVITY) {
            // return 29;
            return 40;
        }
        return 60;
    }

    private int getMinNodeSep() {
        if (umlDiagramType == UmlDiagramType.ACTIVITY) {
            // return 15;
            return 20;
        }
        return 35;
    }

    private GraphvizVersion graphvizVersion;

    public GraphvizVersion getGraphvizVersion() {
        if (graphvizVersion == null) {
            graphvizVersion = getGraphvizVersionInternal();
        }
        return graphvizVersion;
    }

    private GraphvizVersion getGraphvizVersionInternal() {
        final Graphviz graphviz = GraphvizUtils.create(skinParam, "foo;", "svg");
        if (graphviz instanceof GraphvizJs) {
            return GraphvizJs.getGraphvizVersion(false);
        }
        final File f = graphviz.getDotExe();
        return GraphvizVersions.getInstance().getVersion(f);
    }

    public String getSvg(BaseFile basefile, String[] dotOptions) throws IOException {
        String dotString = createDotString(dotOptions);

        if (basefile != null) {
            final File f = basefile.getTraceFile("svek.dot");
            SvekUtils.traceString(f, dotString);
        }

        Graphviz graphviz = GraphvizUtils.create(skinParam, dotString, "svg");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            final ProcessState state = graphviz.createFile3(baos);
            baos.close();
            if (state.differs(ProcessState.TERMINATED_OK())) {
                throw new IllegalStateException("Timeout4 " + state, state.getCause());
            }
        } catch (GraphvizJsRuntimeException e) {
            System.err.println("GraphvizJsRuntimeException");
            graphvizVersion = GraphvizJs.getGraphvizVersion(true);
            dotString = createDotString(dotOptions);
            graphviz = GraphvizUtils.create(skinParam, dotString, "svg");
            baos = new ByteArrayOutputStream();
            final ProcessState state = graphviz.createFile3(baos);
            baos.close();
            if (state.differs(ProcessState.TERMINATED_OK())) {
                throw new IllegalStateException("Timeout4 " + state, state.getCause());
            }
        }
        final byte[] result = baos.toByteArray();
        final String s = new String(result, "UTF-8");

        if (basefile != null) {
            final File f = basefile.getTraceFile("svek.svg");
            SvekUtils.traceString(f, s);
        }

        return s;
    }

    public boolean illegalDotExe() {
        final Graphviz graphviz = GraphvizUtils.create(skinParam, "svg");
        if (graphviz instanceof GraphvizJs || graphviz instanceof JGraphviz) {
            return false;
        }
        final File dotExe = graphviz.getDotExe();
        return dotExe == null || dotExe.isFile() == false || dotExe.canRead() == false;
    }

    public File getDotExe() {
        final Graphviz graphviz = GraphvizUtils.create(skinParam, "svg");
        return graphviz.getDotExe();
    }

    public ClusterPosition solve(String svg) throws IOException, InterruptedException {
        if (svg.length() == 0) {
            throw new EmptySvgException();
        }

        final Pattern pGraph = Pattern.compile("(?m)\\<svg\\s+width=\"(\\d+)pt\"\\s+height=\"(\\d+)pt\"");
        final Matcher mGraph = pGraph.matcher(svg);
        if (mGraph.find() == false) {
            throw new IllegalStateException();
        }
        final int fullWidth = Integer.parseInt(mGraph.group(1));
        final int fullHeight = Integer.parseInt(mGraph.group(2));

        final MinFinder corner1 = new MinFinder();

        for (Shape sh : bibliotekon.allShapes()) {
            int idx = svg.indexOf("<title>" + sh.getUid() + "</title>");
            if (sh.getType() == ShapeType.RECTANGLE || sh.getType() == ShapeType.RECTANGLE_HTML_FOR_PORTS
                    || sh.getType() == ShapeType.FOLDER || sh.getType() == ShapeType.DIAMOND) {
                final List<Point2D.Double> points = SvekUtils.extractPointsList(svg, idx, fullHeight);
                final double minX = SvekUtils.getMinX(points);
                final double minY = SvekUtils.getMinY(points);
                corner1.manage(minX, minY);
                sh.moveSvek(minX, minY);
            } else if (sh.getType() == ShapeType.ROUND_RECTANGLE) {
                final int idx2 = svg.indexOf("d=\"", idx + 1);
                idx = svg.indexOf("points=\"", idx + 1);
                final List<Point2D.Double> points;
                if (idx2 != -1 && (idx == -1 || idx2 < idx)) {
                    // GraphViz 2.30
                    points = SvekUtils.extractD(svg, idx2, fullHeight);
                } else {
                    points = SvekUtils.extractPointsList(svg, idx, fullHeight);
                    for (int i = 0; i < 3; i++) {
                        idx = svg.indexOf("points=\"", idx + 1);
                        points.addAll(SvekUtils.extractPointsList(svg, idx, fullHeight));
                    }
                }
                final double minX = SvekUtils.getMinX(points);
                final double minY = SvekUtils.getMinY(points);
                corner1.manage(minX, minY);
                sh.moveSvek(minX, minY);
            } else if (sh.getType() == ShapeType.OCTAGON) {
                idx = svg.indexOf("points=\"", idx + 1);
                final List<Point2D.Double> points = SvekUtils.extractPointsList(svg, idx, fullHeight);
                final double minX = SvekUtils.getMinX(points);
                final double minY = SvekUtils.getMinY(points);
                corner1.manage(minX, minY);
                sh.moveSvek(minX, minY);
                sh.setOctagon(minX, minY, points);
            } else if (sh.getType() == ShapeType.CIRCLE || sh.getType() == ShapeType.CIRCLE_IN_RECT
                    || sh.getType() == ShapeType.OVAL) {
                final double cx = SvekUtils.getValue(svg, idx, "cx");
                final double cy = SvekUtils.getValue(svg, idx, "cy") + fullHeight;
                final double rx = SvekUtils.getValue(svg, idx, "rx");
                final double ry = SvekUtils.getValue(svg, idx, "ry");
                sh.moveSvek(cx - rx, cy - ry);
            } else {
                throw new IllegalStateException(sh.getType().toString() + " " + sh.getUid());
            }
        }

        for (Cluster cluster : bibliotekon.allCluster()) {
            int idx = getClusterIndex(svg, cluster.getColor());
            final List<Point2D.Double> points = SvekUtils.extractPointsList(svg, idx, fullHeight);
            final double minX = SvekUtils.getMinX(points);
            final double minY = SvekUtils.getMinY(points);
            final double maxX = SvekUtils.getMaxX(points);
            final double maxY = SvekUtils.getMaxY(points);
            cluster.setPosition(minX, minY, maxX, maxY);
            corner1.manage(minX, minY);

            if (cluster.getTitleAndAttributeWidth() == 0 || cluster.getTitleAndAttributeHeight() == 0) {
                continue;
            }
            idx = getClusterIndex(svg, cluster.getTitleColor());
            final List<Point2D.Double> pointsTitle = SvekUtils.extractPointsList(svg, idx, fullHeight);
            final double minXtitle = SvekUtils.getMinX(pointsTitle);
            final double minYtitle = SvekUtils.getMinY(pointsTitle);
            cluster.setTitlePosition(minXtitle, minYtitle);
        }

        for (Line line : bibliotekon.allLines()) {
            line.solveLine(svg, fullHeight, corner1);
        }

        for (Line line : bibliotekon.allLines()) {
            line.manageCollision(bibliotekon.allShapes());
        }
        corner1.manage(0, 0);
        return new ClusterPosition(corner1.getMinX(), corner1.getMinY(), fullWidth, fullHeight);
        // return new ClusterPosition(0, 0, fullWidth, fullHeight);
    }

    private int getClusterIndex(final String svg, int colorInt) {
        final String colorString = StringUtils.goLowerCase(StringUtils.getAsHtml(colorInt));
        final String keyTitle1 = "=\"" + colorString + "\"";
        int idx = svg.indexOf(keyTitle1);
        if (idx == -1) {
            final String keyTitle2 = "stroke:" + colorString + ";";
            idx = svg.indexOf(keyTitle2);
        }
        if (idx == -1) {
            throw new IllegalStateException("Cannot find color " + colorString);
        }
        return idx;
    }

    public void openCluster(IGroup g, int titleAndAttributeWidth, int titleAndAttributeHeight, TextBlock title,
                            TextBlock stereo) {
        this.current = current.createChild(g, titleAndAttributeWidth, titleAndAttributeHeight, title, stereo,
                colorSequence, skinParam);
        bibliotekon.addCluster(this.current);
    }

    public void closeCluster() {
        if (current.getParent() == null) {
            throw new IllegalStateException();
        }
        this.current = current.getParent();
    }

    public void moveSvek(double deltaX, double deltaY) {
        for (Shape sh : bibliotekon.allShapes()) {
            sh.moveSvek(deltaX, deltaY);
        }
        for (Line line : bibliotekon.allLines()) {
            line.moveSvek(deltaX, deltaY);
        }
        for (Cluster cl : bibliotekon.allCluster()) {
            cl.moveSvek(deltaX, deltaY);
        }

    }

    public final Bibliotekon getBibliotekon() {
        return bibliotekon;
    }

    public ColorSequence getColorSequence() {
        return colorSequence;
    }

}

