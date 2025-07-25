package com.boots.dsp.messageconverter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Objects;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@SpringBootApplication
public class DemoApplication implements CommandLineRunner {

	@Value ("${transform.source.folder}")
	private String transformSourceFolder;
	
	@Value ("${transform.destination.folder}")
	private String transformDestinationFolder;
	
	@Value ("${web.service.url}")
	private String webServiceUrl;
	
	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}
	
	@Override
	public void run(String... args) throws Exception {
		String mode = getModeFromArgs(args);
		System.out.println("Using command mode " + mode);
		if ("enqueueFiles".equalsIgnoreCase(mode)) {
			enqueueMessagesFromFiles();
		} else {
			transformFiles();
		}
	}
	
	private String getModeFromArgs(String[] args) {
		return Arrays.stream(args)
				.filter(arg -> arg.startsWith("--mode="))
				.map(arg -> arg.substring("--mode=".length()))
				.findFirst()
				.orElse("transform");
	}
	
	private void transformFiles() {
		File srcDir = new File(transformSourceFolder);
		File destDir = new File(transformDestinationFolder);
		
		if (!destDir.exists()) {
			destDir.mkdirs();
		}
		
		RestTemplate restTemplate = new RestTemplate();
		
		for (File file : Objects.requireNonNull(srcDir.listFiles((dir, name) -> name.endsWith(".txt")))) {
			System.out.println("Processing file: " + file.getName());
			MultiValueMap<String, Object> requestBody = new LinkedMultiValueMap<>();
			requestBody.add("file", new FileSystemResource(file));
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.MULTIPART_FORM_DATA);
			HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(requestBody, headers);
			ResponseEntity<String> response = restTemplate.postForEntity(webServiceUrl, requestEntity, String.class);
			if (response.getStatusCode().is2xxSuccessful()) {
				System.out.println("File processed successfully: " + file.getName());
				String outputFileName = file.getName().replaceAll("\\.txt$", ".json");
				File destFile = new File(destDir, outputFileName);
				try (FileWriter writer = new FileWriter(destFile)) {
					writer.write(response.getBody());
					System.out.println("Output written to: " + destFile.getAbsolutePath());
				} catch (IOException e) {
					System.err.println("Error writing to file: " + destFile.getAbsolutePath());
				}
			} else {
				System.out.println("Failed to process file: " + file.getName() + ", Status code: " + response.getStatusCode());
			}
		}		
	}
	
	@Autowired
	private RabbitTemplate rabbitTemplate;
	
	@Value("${rabbitmq.queue}")
	private String queueName;
	
	@Value("${metadata.sourceSystem}")
	private String sourceSystem;
	
	@Value("${metadata.destinationAddress}")
	private String destinationAddress;
	
	@Value("${metadata.messageRenderTechnology}")
	private String messageRenderTechnology;
	
	@Value("${metadata.formatURL}")
	private String formatURL;
	
	@Value("${upload.source.folder}")
	private String uploadSource;
	
	@Value("${upload.filename.suffix}")
	private String uploadSuffix;
	
	private void enqueueMessagesFromFiles() {
		File dir = new File(uploadSource);
		ObjectMapper mapper = new ObjectMapper();
		for (File file : Objects.requireNonNull(dir.listFiles((d, n) -> n.endsWith("." + uploadSuffix)))) {
			try {
				String content = Files.readString(file.toPath(), StandardCharsets.UTF_8);
				String baseName = file.getName().replaceFirst("\\." + uploadSuffix + "$", "");
				ObjectNode wrapper = mapper.createObjectNode();
				wrapper.put("sourceSystem", sourceSystem);
				wrapper.put("destinationAddress", destinationAddress);
				wrapper.put("messageId", "msg-" + baseName);
				wrapper.put("correlationId", "corr-" + baseName);
				wrapper.put("messageRenderTechnology", messageRenderTechnology);
				wrapper.put("formatUrl", formatURL);
				wrapper.put("payload", content);
				
				MessageProperties props = new MessageProperties();
				props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
				Message message = new Message(wrapper.toString().getBytes(StandardCharsets.UTF_8), props);
				rabbitTemplate.send(queueName, message);
				System.out.println("Enqueued message: " + file.getName());
			} catch (IOException e) {
				System.err.println("Error reading file: " + file.getAbsolutePath());
			} catch (Exception e) {
				System.err.println("Error enqueuing message: " + e.getMessage());
			}	
		}
	}
}
