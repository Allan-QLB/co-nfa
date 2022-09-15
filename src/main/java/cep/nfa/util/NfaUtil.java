package cep.nfa.util;

import cep.nfa.NFA;

import java.io.IOException;

public class NfaUtil {

    public static void drawGraphic(NFA<?> nfa) throws IOException {
        Drawer.drawNFAGraph(nfa);
    }



}
