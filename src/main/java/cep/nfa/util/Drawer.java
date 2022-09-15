package cep.nfa.util;

import cep.nfa.NFA;
import cep.nfa.State;
import cep.nfa.StateTransition;
import cn.hutool.core.collection.CollectionUtil;
import net.sourceforge.plantuml.SourceStringReader;

import javax.imageio.ImageIO;
import javax.swing.*;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;

public class Drawer {
    private static final String START = "@startuml\n";
    private static final String END = "@enduml\n";
    private static final String TITLE = "NFA Graph";
    public static void drawNFAGraph(NFA<?> nfa) throws IOException {
        final BufferedImage image = createImageFromCode(buildSource(headState(nfa)));
        showImage(image);
    }

    private static void showImage(BufferedImage image) {
        JFrame frame = new JFrame(TITLE);
        JPanel panel = new JPanel();
        JLabel label = new JLabel();
        label.setIcon(new ImageIcon(image));
        panel.add(label);
        frame.add(panel);
        frame.pack();
        adjustToScreenCenter(image, frame);
        frame.setVisible(true);
    }

    private static void adjustToScreenCenter(BufferedImage image, JFrame frame) {
        final int height = Toolkit.getDefaultToolkit().getScreenSize().height;
        final int width = Toolkit.getDefaultToolkit().getScreenSize().width;
        final int h = image.getHeight();
        final int w = image.getWidth();
        frame.setLocation((width - w) /2, (height - h) /2);
    }

    private static BufferedImage createImageFromCode(String source) throws IOException {
        ByteArrayOutputStream img = new ByteArrayOutputStream();
        SourceStringReader reader = new SourceStringReader(source);
        reader.generateImage(img);
        return ImageIO.read(new ByteArrayInputStream(img.toByteArray()));
    }

    private static String buildSource(State<?> head) {
        final StringBuilder sb = new StringBuilder();
        sb.append(START);
        buildSource(head, sb);
        sb.append(END);
        return sb.toString();
    }

    private static void buildSource(State<?> current, StringBuilder sb) {
        if (current != null && !current.isFinal()) {
            final Collection<? extends StateTransition<?>> stateTransitions = current.getStateTransitions();
            if (CollectionUtil.isNotEmpty(stateTransitions)) {
                for (StateTransition<?> stateTransition : stateTransitions) {
                    final State<?> targetState = stateTransition.getTargetState();
                    final State<?> sourceState = stateTransition.getSourceState();
                    String connector;
                    if (targetState != null) {
                        switch (stateTransition.getAction()) {
                            case TAKE:
                                connector = "-->";
                                break;
                            case PROCEED:
                                connector = "..|>";
                                break;
                            case IGNORE:
                                connector = "..>";
                                break;
                            default:
                                connector = "->";
                        }
                        sb.append(sourceState.getName().replace(":", "$") + connector
                                + targetState.getName().replace(":", "$") + "\n");
                        if (sourceState != targetState) {
                            buildSource(targetState, sb);
                        }
                    }
                }
            }
        }

    }


    private static State<?> headState(NFA<?> nfa) {
        final Collection<? extends State<?>> states = nfa.getStates();
        for (State<?> state : states) {
            if (state.isStart()) {
                return state;
            }
        }
        return null;
    }
}
