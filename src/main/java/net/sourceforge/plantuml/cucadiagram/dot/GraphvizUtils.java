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
package net.sourceforge.plantuml.cucadiagram.dot;

import net.sourceforge.plantuml.ISkinParam;
import net.sourceforge.plantuml.Log;
import net.sourceforge.plantuml.StringUtils;
import net.sourceforge.plantuml.vizjs.GraphvizJs;
import net.sourceforge.plantuml.vizjs.VizJsEngine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GraphvizUtils {

    private static final String VIZJS = "vizjs";
    private static int DOT_VERSION_LIMIT = 226;

    private static boolean isWindows() {
        return File.separatorChar == '\\';
    }

    private static String dotExecutable;

    public static final String getDotExecutableForTest() {
        return dotExecutable;
    }

    public static final void setDotExecutable(String value) {
        dotExecutable = value;
    }

    public static Graphviz create(ISkinParam skinParam, String dotString, String... type) {
        if (useVizJs(skinParam)) {
            Log.info("Using " + VIZJS);
            return new GraphvizJs(dotString);
        }
        if (useJava()) {
            return new JGraphviz(dotString);
        }
        final AbstractGraphviz result;
        if (isWindows()) {
            result = new GraphvizWindows(skinParam, dotString, type);
        } else {
            result = new GraphvizLinux(skinParam, dotString, type);
        }
        if (result.getExeState() != ExeState.OK && VizJsEngine.isOk()) {
            Log.info("Error with file " + result.getDotExe() + ": " + result.getExeState().getTextMessage());
            Log.info("Using " + VIZJS);
            return new GraphvizJs(dotString);
        }
        return result;
    }

    private static boolean useJava() {
        try {
            Class.forName("guru.nidi.graphviz.engine.Graphviz");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static boolean useVizJs(ISkinParam skinParam) {
        if (skinParam != null && VIZJS.equalsIgnoreCase(skinParam.getDotExecutable()) && VizJsEngine.isOk()) {
            return true;
        }
        if (VIZJS.equalsIgnoreCase(getenvGraphvizDot()) && VizJsEngine.isOk()) {
            return true;
        }
        return false;
    }

    static public File getDotExe() {
        return create(null, "png").getDotExe();
    }

    public static String getenvGraphvizDot() {
        if (StringUtils.isNotEmpty(dotExecutable)) {
            return StringUtils.eventuallyRemoveStartingAndEndingDoubleQuote(dotExecutable);
        }
        final String env = System.getProperty("GRAPHVIZ_DOT");
        if (StringUtils.isNotEmpty(env)) {
            return StringUtils.eventuallyRemoveStartingAndEndingDoubleQuote(env);
        }
        final String getenv = System.getenv("GRAPHVIZ_DOT");
        if (StringUtils.isNotEmpty(getenv)) {
            return StringUtils.eventuallyRemoveStartingAndEndingDoubleQuote(getenv);
        }
        return null;
    }

    public static int getenvImageLimit() {
        final String env = System.getProperty("PLANTUML_LIMIT_SIZE");
        if (StringUtils.isNotEmpty(env) && env.matches("\\d+")) {
            return Integer.parseInt(env);
        }
        final String getenv = System.getenv("PLANTUML_LIMIT_SIZE");
        if (StringUtils.isNotEmpty(getenv) && getenv.matches("\\d+")) {
            return Integer.parseInt(getenv);
        }
        return 4096;
    }

    public static String getenvLogData() {
        final String env = System.getProperty("PLANTUML_LOGDATA");
        if (StringUtils.isNotEmpty(env)) {
            return env;
        }
        return System.getenv("PLANTUML_LOGDATA");
    }

    private static String dotVersion = null;

    public static String dotVersion() throws IOException, InterruptedException {
        if (dotVersion == null) {
            final File dotExe = GraphvizUtils.getDotExe();
            final ExeState exeState = ExeState.checkFile(dotExe);
            if (exeState == ExeState.OK) {
                dotVersion = create(null, "png").dotVersion();
            } else {
                dotVersion = "Error:" + exeState.getTextMessage(dotExe);
            }
        }
        return dotVersion;
    }

    public static int retrieveVersion(String s) {
        if (s == null) {
            return -1;
        }
        final Pattern p = Pattern.compile("\\s([12].\\d\\d)\\D");
        final Matcher m = p.matcher(s);
        if (m.find() == false) {
            return -1;
        }
        return Integer.parseInt(m.group(1).replaceAll("\\.", ""));
    }

    public static int getDotVersion() throws IOException, InterruptedException {
        return retrieveVersion(dotVersion());
    }

    static public List<String> getTestDotStrings(boolean withRichText) {
        String red = "";
        String bold = "";
        if (withRichText) {
            red = "<b><color:red>";
            bold = "<b>";
        }

        final List<String> result = new ArrayList<String>();
        if (useVizJs(null)) {
            result.add("VizJs library is used!");
            try {
                final String err = getTestCreateSimpleFile();
                if (err == null) {
                    result.add(bold + "Installation seems OK. File generation OK");
                } else {
                    result.add(red + err);
                }
            } catch (Exception e) {
                result.add(red + e.toString());
                e.printStackTrace();
            }
            return Collections.unmodifiableList(result);
        }

        final String ent = GraphvizUtils.getenvGraphvizDot();
        if (ent == null) {
            result.add("The environment variable GRAPHVIZ_DOT has not been set");
        } else {
            result.add("The environment variable GRAPHVIZ_DOT has been set to " + ent);
        }
        final File dotExe = GraphvizUtils.getDotExe();
        result.add("Dot executable is " + dotExe);

        final ExeState exeState = ExeState.checkFile(dotExe);

        if (exeState == ExeState.OK) {
            try {
                final String version = GraphvizUtils.dotVersion();
                result.add("Dot version: " + version);
                final int v = GraphvizUtils.getDotVersion();
                if (v == -1) {
                    result.add("Warning : cannot determine dot version");
                } else if (v < DOT_VERSION_LIMIT) {
                    result.add(bold + "Warning : Your dot installation seems old");
                    result.add(bold + "Some diagrams may have issues");
                } else {
                    final String err = getTestCreateSimpleFile();
                    if (err == null) {
                        result.add(bold + "Installation seems OK. File generation OK");
                    } else {
                        result.add(red + err);
                    }
                }
            } catch (Exception e) {
                result.add(red + e.toString());
                e.printStackTrace();
            }
        } else {
            result.add(red + "Error: " + exeState.getTextMessage());
            result.add("Error: only sequence diagrams will be generated");
        }

        return Collections.unmodifiableList(result);
    }

    static String getTestCreateSimpleFile() throws IOException {
        final Graphviz graphviz2 = GraphvizUtils.create(null, "digraph foo { test; }", "svg");
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ProcessState state = graphviz2.createFile3(baos);
        if (state.differs(ProcessState.TERMINATED_OK())) {
            return "Error: timeout " + state;
        }

        final byte data[] = baos.toByteArray();

        if (data.length == 0) {
            return "Error: dot generates empty file. Check you dot installation.";
        }
        final String s = new String(data);
        if (s.indexOf("<svg") == -1) {
            return "Error: dot generates unreadable SVG file. Check you dot installation.";
        }
        return null;
    }

}
