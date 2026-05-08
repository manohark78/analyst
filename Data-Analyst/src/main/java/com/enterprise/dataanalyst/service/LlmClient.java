package com.enterprise.dataanalyst.service;

import de.kherud.llama.LlamaModel;
import de.kherud.llama.ModelParameters;
import de.kherud.llama.InferenceParameters;
import de.kherud.llama.LlamaOutput;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

@Slf4j
@Service
public class LlmClient {

    @Value("${app.llm.model-path}")
    private String modelPath;

    @Value("${app.llm.gpu-layers:0}")
    private int gpuLayers;

    @Value("${app.llm.context-size:4096}")
    private int contextSize;

    private LlamaModel model;

    // Concurrency guard: only 1 request at a time to prevent JNI crashes
    private final Semaphore generationLock = new Semaphore(1);

    @PostConstruct
    public void init() {
        File file = new File(modelPath);
        if (!file.exists()) {
            log.warn("Model file not found at {}. AI will remain offline until downloaded.", file.getAbsolutePath());
            return;
        }

        try {
            log.info("Loading LLM model from {} with {} GPU layers and {} context...", modelPath, gpuLayers,
                    contextSize);
            ModelParameters params = new ModelParameters()
                    .setModel(file.getAbsolutePath())
                    .setGpuLayers(gpuLayers)
                    .setCtxSize(contextSize)
                    .setThreads(Runtime.getRuntime().availableProcessors());

            this.model = new LlamaModel(params);
            log.info("LLM model loaded successfully into memory.");
        } catch (Exception e) {
            log.error("Failed to load LlamaModel via JNI", e);
        }
    }

    @PreDestroy
    public void close() {
        if (this.model != null) {
            log.info("Closing LLM model and freeing native memory...");
            this.model.close();
        }
    }

    public boolean isModelLoaded() {
        return this.model != null;
    }

    public Flux<String> streamChat(List<Map<String, String>> messages) {
        if (model == null) {
            return Flux.just("\n\n⚠️ **AI Offline:** The local model is not loaded. Please check the model file.");
        }

        String prompt = buildPrompt(messages);

        return Flux.<String>create(sink -> {
            try {
                // Acquire lock so JNI doesn't crash from concurrent generations
                generationLock.acquire();

                InferenceParameters inferParams = new InferenceParameters(prompt)
                        .setTemperature(0.1f)
                        .setNPredict(2048);

                for (LlamaOutput output : model.generate(inferParams)) {
                    if (sink.isCancelled())
                        break;
                    sink.next(output.text);
                }
                sink.complete();
            } catch (Exception e) {
                log.error("Error generating stream", e);
                sink.error(e);
            } finally {
                generationLock.release();
            }
        }).subscribeOn(Schedulers.boundedElastic()); // Run on background thread so we don't block Netty
    }

    public String complete(List<Map<String, String>> messages) {
        if (model == null) {
            return "Error: Model offline.";
        }

        String prompt = buildPrompt(messages);
        StringBuilder response = new StringBuilder();

        try {
            generationLock.acquire();
            InferenceParameters inferParams = new InferenceParameters(prompt)
                    .setTemperature(0.0f)
                    .setNPredict(512);

            for (LlamaOutput output : model.generate(inferParams)) {
                response.append(output.text);
            }
        } catch (Exception e) {
            log.error("Error generating completion", e);
            return "Error: Unable to process request.";
        } finally {
            generationLock.release();
        }

        return response.toString();
    }

    /**
     * Converts a list of messages into Qwen/ChatML prompt format.
     */
    private String buildPrompt(List<Map<String, String>> messages) {
        StringBuilder sb = new StringBuilder();
        for (Map<String, String> msg : messages) {
            String role = msg.get("role");
            String content = msg.get("content");
            sb.append("<|im_start|>").append(role).append("\n");
            sb.append(content).append("<|im_end|>\n");
        }
        sb.append("<|im_start|>assistant\n");
        return sb.toString();
    }
}
