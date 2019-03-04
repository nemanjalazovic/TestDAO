package pkg;

import java.sql.SQLException;
import java.util.ArrayList;

import javax.security.sasl.AuthorizeCallback;

public interface IAuthorDAO {
	public String GetVersion() throws SQLException;
	public Author getAuthor(int id);
	public ArrayList<Author> getAllAuthors();
	public boolean insertAuthor(Author author);
	public boolean updateAuthor(Author author);
	public boolean deleteAuthor(int id);
	public void writeMultipleRows(String query) throws SQLException;
	public void writeMetaData(String query) throws SQLException;
	
	

}
