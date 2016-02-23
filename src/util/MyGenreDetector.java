package util;

import scala.Tuple2;

public class MyGenreDetector extends GenreDetector {
	
	public Tuple2<Genre, float[]> detectGenreCrossover(String[] terms, float[] importances)
	{
		float maxImportance = 0.0f;
		Genre maxGenre = Genre.OTHER;
		float[] genreImportances = new float[GENRES_COUNT];
		
		for (int i=0; i<GENRES_COUNT; i++)
		{
			genreImportances[i] = 0.0f;
		}
		for (int i=0; i<terms.length; i++)
		{
			updateFromTerm(genreImportances, terms[i], importances[i]);
		}
		
		// Choose the genre as the one with the maximum accumulated importance.
		for (Genre genre: Genre.values())
		{
			float crtImportance = genreImportances[genre.ordinal()];
			if (crtImportance > maxImportance)
			{
				maxImportance = crtImportance;
				maxGenre = genre;
			}
		}

		return new Tuple2<Genre, float[]>(maxGenre, genreImportances);
	}
}
