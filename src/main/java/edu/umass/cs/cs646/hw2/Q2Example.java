package edu.umass.cs.cs646.hw2;

import edu.umass.cs.cs646.utils.EvalUtils;
import edu.umass.cs.cs646.utils.LuceneSearchUtils;
import edu.umass.cs.cs646.utils.LuceneUtils;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Q2Example {
	
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
			
			LuceneSearchUtils.BestMatchSearch daat = new LuceneSearchUtils.DocAtATime();
			LuceneSearchUtils.BestMatchSearch taat2 = new LuceneSearchUtils.TermAtATime2();
			
			// just use RawTF and IDF for this experiment
			LuceneSearchUtils.DocumentDependentWeight dd_tf = new LuceneSearchUtils.RawTF();
			LuceneSearchUtils.DocumentIndependentWeight di_idf = new LuceneSearchUtils.IDF();
			
			int n = 10;
			
			System.out.printf( "%-10s%15s%15s\n", "round", "DocAtATime", "TermAtATime2" );
			
			LuceneSearchUtils.BestMatchSearch[] searches = new LuceneSearchUtils.BestMatchSearch[]{
					daat, taat2
			};
			double[][] times = new double[searches.length][n];
			List<Integer> sequence = new ArrayList<>();
			for ( int ix = 0; ix < searches.length; ix++ ) {
				sequence.add( ix );
			}
			
			for ( int round = -1; round < n; round++ ) {
				
				// let's shuffle the sequence in each round to make it fair
				Collections.shuffle( sequence );
				for ( int seq : sequence ) {
					if ( round >= 0 ) {
						times[seq][round] = System.nanoTime();
					}
					for ( String qid : queries.keySet() ) {
						String query = queries.get( qid );
						List<String> terms = LuceneUtils.tokenize( query, analyzer );
						searches[seq].search( index, field_search, terms, n, di_idf, dd_tf );
					}
					if ( round >= 0 ) {
						times[seq][round] = ( System.nanoTime() - times[seq][round] ) / 1000000;
					}
				}
				
				if ( round >= 0 ) {
					System.out.printf( "%-10d", ( round + 1 ) );
					for ( int ix = 0; ix < searches.length; ix++ ) {
						System.out.printf( "%13.0fms", times[ix][round] );
					}
					System.out.println();
				}
			}
			
			System.out.printf( "%-10s", "mean" );
			for ( int ix = 0; ix < searches.length; ix++ ) {
				System.out.printf( "%13.0fms", StatUtils.mean( times[ix] ) );
			}
			System.out.println();
			
			System.out.printf( "%-10s", "std.dev" );
			for ( int ix = 0; ix < searches.length; ix++ ) {
				System.out.printf( "%13.0fms", Math.pow( StatUtils.variance( times[ix] ), 0.5 ) );
			}
			System.out.println();
			
			index.close();
			dir.close();
			
		} catch ( Exception e ) {
			e.printStackTrace();
		}
	}
	
}
