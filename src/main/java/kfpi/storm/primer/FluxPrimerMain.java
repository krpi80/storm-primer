package kfpi.storm.primer;

import org.apache.storm.flux.Flux;

public class FluxPrimerMain {
    public static void main(String[] args) throws Exception {
        Flux.main(new String[]{
                "-l",
                "-R",
                "/primer.yaml"
        });
    }
}
