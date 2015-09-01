
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;

public class Node {
	
	private Integer m_id;
	private Map<Integer, Integer> m_adjWeight = new TreeMap<Integer, Integer>();
	
	public Node(Integer id)
	{
		m_id = id;
	}
	
	public Node(String record)
	{
		if(record.endsWith("*"))
			record = record.substring(0, record.length()-2);
			
		String[] pair = record.split("\t+");
		
		String key = pair[0].trim();
		String value = pair[1].trim();

		m_id = Integer.parseInt(key);
		
		//populating node with edges and weights
		for(String v: value.split("\\|")){

			if( !v.trim().isEmpty() )
			{
				String[] vpair = v.split(",");
				m_adjWeight.put(Integer.parseInt(vpair[0].trim()), Integer.parseInt(vpair[1].trim()));
			}
		}
	}
	
	public Integer getId()
	{
		return m_id;
		
	}
	
	public Set<Integer> getAdj()
	{
		return m_adjWeight.keySet();
	}
	
	public void setAdjWeight(Integer adj, Integer weight){
		m_adjWeight.put(adj, weight);
	}
	
	public Integer getAdjWeight(Integer adj){
		Integer w = m_adjWeight.get(adj);
		return w==null?Integer.MAX_VALUE:w;
	}

	public Text getFormattedValue(boolean mark){
		StringBuffer s = new StringBuffer();
		String result;
		
		//append the edges and weights
		for(Integer adj : m_adjWeight.keySet()){
			s.append(adj).append(",").append(m_adjWeight.get(adj));
			s.append("|");
		}
		
		if(mark)
			s.append('*');
		
		result = "\t"+s.toString().trim();
		s = null;
		
		return new Text(result);
	}
	
	public int getAdjSize()
	{
		return m_adjWeight.size();
	}
}
