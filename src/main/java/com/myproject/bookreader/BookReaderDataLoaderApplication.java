package com.myproject.bookreader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import com.myproject.bookreader.author.Author;
import com.myproject.bookreader.author.AuthorRepository;
import com.myproject.bookreader.book.Book;
import com.myproject.bookreader.book.BookRepository;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import connection.DataStaxAstraProperties;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BookReaderDataLoaderApplication {

	@Autowired
	private AuthorRepository authorRepository;

	@Autowired
	private BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.work}")
	private String workDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BookReaderDataLoaderApplication.class, args);
	}

	private void initAuthor() {
		Path path = Paths.get(authorDumpLocation);
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				// read and parse the line
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);

					Author author = new Author();
					author.setId(jsonObject.optString("key").replace("/authors/", ""));
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));
					System.out.println("saving");
					authorRepository.save(author);
					// construct author object
					// Persist using repository
				} catch (Exception e) {
					e.printStackTrace();
				}

			});
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private void initWorks() {

		DateTimeFormatter  datePattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		Path path = Paths.get(workDumpLocation);
		try (Stream<String> lines = Files.lines(path)) {
			lines.forEach(line -> {
				// read and parse the line
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);

					Book book = new Book();
					book.setId(jsonObject.getString("key").replace("/works/", ""));
					book.setName(jsonObject.optString("title"));
					JSONObject disJsonObject = jsonObject.optJSONObject("description");
					if (disJsonObject != null) {
						book.setDescription(disJsonObject.optString("value"));
					}

					JSONArray authorJsonObject = jsonObject.optJSONArray("authors");
					if (authorJsonObject != null) {
						List<String> authorIds = new ArrayList<>();
						for (int i = 0; i < authorJsonObject.length(); i++) {
							authorIds.add(authorJsonObject.getJSONObject(i).getJSONObject("author").getString("key")
									.replace("/authors/", ""));
						}
						book.setAuthorIds(authorIds);

						List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
								.map(optionalAuthor -> {
									if (!optionalAuthor.isPresent())
										return "Unknowed Author";
									return optionalAuthor.get().getName();
								}).collect(Collectors.toList());

						book.setAuthorNames(authorNames);

					}

					JSONArray coverJsonObject = jsonObject.optJSONArray("covers");
					if (coverJsonObject != null) {
						List<String> coverId = new ArrayList<>();
						for (int i = 0; i < coverJsonObject.length(); i++) {
							coverId.add(coverJsonObject.getString(i));
						}
						book.setCoverIds(coverId);
					}

					JSONObject publishJsonObject = jsonObject.optJSONObject("created");
					if (publishJsonObject != null) {
						book.setPublishedDate(LocalDate.parse(publishJsonObject.optString("value"),datePattern));
					}
					System.out.println("saving book");
					bookRepository.save(book);
					// construct author object
					// Persist using repository
				} catch (Exception e) {
					e.printStackTrace();
				}

			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@PostConstruct
	public void start() {
		initAuthor();
		initWorks();
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}
