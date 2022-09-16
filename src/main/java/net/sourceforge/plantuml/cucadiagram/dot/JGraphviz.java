package net.sourceforge.plantuml.cucadiagram.dot;
import guru.nidi.graphviz.engine.*;

import java.io.File;
import java.io.OutputStream;

public class JGraphviz implements Graphviz {
    private final String dotString;
    private final GraphvizEngine engine = new GraphvizV8Engine();
    public JGraphviz(String dotString) {
        this.dotString = dotString;
    }

    @Override
    public ProcessState createFile3(OutputStream os) {
        final EngineResult execute = engine.execute(dotString, Options.create(), Rasterizer.DEFAULT);
        try {
            os.write(execute.asString().getBytes());
            return ProcessState.TERMINATED_OK();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public File getDotExe() {
        return null;
    }

    @Override
    public String dotVersion() {
        return "JGraphviz";
    }

    @Override
    public ExeState getExeState() {
        return ExeState.OK;
    }
}
