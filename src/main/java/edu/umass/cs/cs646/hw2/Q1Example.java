package edu.umass.cs.cs646.hw2;

import edu.umass.cs.cs646.utils.EvalUtils;
import edu.umass.cs.cs646.utils.LuceneSearchUtils;
import edu.umass.cs.cs646.utils.LuceneUtils;
import edu.umass.cs.cs646.utils.SearchResult;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Q1Example {
	
	public static void main( String[] args ) {
		try {
			
			String pathIndex = "/home/jiepu/Downloads/index_lucene_robust04_krovetz"; // change it to your own index path
			Analyzer analyzer = LuceneUtils.getAnalyzer( LuceneUtils.Stemming.Krovetz ); // change the stemming setting accordingly
			
			String pathQueries = "/home/jiepu/Downloads/queries_short"; // change it to your query file path
			String pathQrels = "/home/jiepu/Downloads/qrels"; // change it to your qrels file path
			
			String field_docno = "docno";
			String field_search = "content";
			
			Directory dir = FSDirectory.open( new File( pathIndex ).toPath() );
			IndexReader index = DirectoryReader.open( dir );
			
			Map<String, String> queries = EvalUtils.loadQueries( pathQueries );
			Map<String, Set<String>> qrels = EvalUtils.loadQrels( pathQrels );
			
			LuceneSearchUtils.BestMatchSearch search = new LuceneSearchUtils.TermAtATime2();
			
			LuceneSearchUtils.DocumentDependentWeight dd_bintf = new LuceneSearchUtils.BinTF();
			LuceneSearchUtils.DocumentDependentWeight dd_rawtf = new LuceneSearchUtils.RawTF();
			
			LuceneSearchUtils.DocumentIndependentWeight di_uni = new LuceneSearchUtils.Uniform();
			LuceneSearchUtils.DocumentIndependentWeight di_idf = new LuceneSearchUtils.IDF();
			
			int n = 1000;
			
			/* Your dd functions. */
			LuceneSearchUtils.DocumentDependentWeight[] dds = new LuceneSearchUtils.DocumentDependentWeight[]{
					dd_bintf, dd_rawtf
			};
			
			/* Your di functions. */
			LuceneSearchUtils.DocumentIndependentWeight[] dis = new LuceneSearchUtils.DocumentIndependentWeight[]{
					di_uni, di_idf
			};
			
			/* The name of your dd functions. */
			String[] dd_names = new String[]{
					"Bin TF", "Raw TF"
			};
			
			/* The name of your di functions. */
			String[] di_names = new String[]{
					"Uniform", "IDF"
			};
			
			double[][][] p10 = new double[dis.length][dds.length][queries.size()];
			double[][][] ap = new double[dis.length][dds.length][queries.size()];
			
			int ix = 0;
			for ( String qid : queries.keySet() ) {
				
				String query = queries.get( qid );
				List<String> terms = LuceneUtils.tokenize( query, analyzer );
				
				for ( int i = 0; i < dis.length; i++ ) {
					for ( int j = 0; j < dds.length; j++ ) {
						
						List<SearchResult> results = search.search( index, field_search, terms, n, dis[i], dds[j] );
						SearchResult.dumpDocno( index, field_docno, results );
						
						p10[i][j][ix] = EvalUtils.precision( results, qrels.get( qid ), 10 );
						ap[i][j][ix] = EvalUtils.avgPrec( results, qrels.get( qid ), n );
					}
				}
				
				ix++;
			}
			
			for ( int i = 0; i < dis.length; i++ ) {
				for ( int j = 0; j < dds.length; j++ ) {
					System.out.printf(
							"%-10s%-25s%10.3f%10.3f\n",
							di_names[i],
							dd_names[j],
							StatUtils.mean( p10[i][j] ),
							StatUtils.mean( ap[i][j] )
					);
				}
			}
			
			index.close();
			dir.close();
			
		} catch ( Exception e ) {
			e.printStackTrace();
		}
	}
	
}
