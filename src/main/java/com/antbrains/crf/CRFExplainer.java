package com.antbrains.crf;

import java.awt.BorderLayout;
import java.awt.Font;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import com.mxgraph.model.mxCell;
import com.mxgraph.swing.mxGraphComponent;
import com.mxgraph.view.mxGraph;


@SuppressWarnings("serial")
public class CRFExplainer extends JFrame{
	private JTextField txtInput=new JTextField(60);
	private JButton btnExplain=new JButton("分词然后分析");
	private JButton btnCalcPath=new JButton("计算当前路径");
	private JTextArea txtResult=new JTextArea(15,80);

	private Explanation explanation;
	private mxGraph graph = new mxGraph();
	private mxGraphComponent graphComponent = new mxGraphComponent(graph);
	private mxCell startCell=null;
	
	private CrfModel model;
	private TagConvertor tc=new BESB1B2MTagConvertor();
	
	private void clickButton(){
		try{
			String s=txtInput.getText();
			if(s!=null&&!s.trim().equals("")){
				explanation=SgdCrf.explain(s, model);
 
				int[] tags=explanation.bestTagIds;
				String[] txtTags=SgdCrf.tagId2Text(tags, model);
				List<String> tks=this.tc.tags2TokenList(txtTags, s);
				StringBuilder sb=new StringBuilder();
				for(String tk:tks){
					sb.append(tk).append(" ");
				}
				
				try{
					txtResult.setText(sb.toString().trim());
				}catch(Exception ex){
					
				}
				draw();
			}
		}catch(Exception e){
		}
	}
	
	
	public CRFExplainer(String crfModelPath) throws Exception{
		super("CRFs分词解析器");
		this.model=SgdCrf.loadModel(crfModelPath);
		try{
			txtInput.setText("今天的天气不错");
		}catch(Exception e){
			
		}
		btnExplain.addActionListener(new ActionListener() {			
			@Override
			public void actionPerformed(ActionEvent actionevent) {
				clickButton();
			}
		});
		
		btnCalcPath.addActionListener(new ActionListener() {			
			@Override
			public void actionPerformed(ActionEvent actionevent) {
				calcCurrentPath();
			}
		});
		
		this.setLayout(new BorderLayout());
		JPanel cmdFrame=new JPanel();
		
		cmdFrame.add(txtInput);
		cmdFrame.add(btnExplain);
		cmdFrame.add(btnCalcPath);
		
		JPanel resultFrame=new JPanel();
		
		Font font=txtResult.getFont();
		Font newFont=new Font(font.getName(),font.getSize(),14);
		txtResult.setFont(newFont);
		JScrollPane scrollPanel=new JScrollPane(txtResult);
		resultFrame.add(scrollPanel);
		
		this.draw();
		graphComponent.getGraphControl().addMouseListener(new MouseAdapter()
		{
			public void mouseReleased(MouseEvent e)
			{	
				Object cell = graphComponent.getCellAt(e.getX(), e.getY());
				if(cell instanceof mxCell){
					mxCell mCell=(mxCell)cell;
					String id=mCell.getId();
					if(id!=null){
						if(mCell.isEdge()){//edge
							if(e.getButton()==MouseEvent.BUTTON3){
								graph.removeCells(new Object[]{cell});
							}else{
								mxCell source=(mxCell) mCell.getSource();
								mxCell target=(mxCell) mCell.getTarget();
								String sId=source.getId();
								String tId=target.getId();
								
								String s="";
								if(sId.equals("vStart")){
									if(tId.equals("vEnd")){
										s="Error Edge";
										graph.removeCells(new Object[]{cell});
									}else{
										int idx=tId.indexOf(",");
										int itemId=Integer.valueOf(tId.substring(1,idx));
										int stateId=Integer.valueOf(tId.substring(idx+1));
										if(itemId!=0){
											s="Error Edge";
											graph.removeCells(new Object[]{cell});
										}else{
											double transitionScore=explanation.bosTransitionWeights[stateId];
											mCell.setValue(df.format(transitionScore));
											graph.removeCells(new Object[]{cell});
											Object parent = graph.getDefaultParent();
											graph.insertEdge(parent, null, df.format(transitionScore), source, target);
										}
									}
								}else if(tId.equals("vEnd")){
									if(sId.equals("vStart")){
										s="Error Edge";
										graph.removeCells(new Object[]{cell});
									}else{
										int idx=sId.indexOf(",");
										int itemId=Integer.valueOf(sId.substring(1,idx));
										int stateId=Integer.valueOf(sId.substring(idx+1));
										if(itemId!=explanation.details.length-1){
											s="Error Edge";
											graph.removeCells(new Object[]{cell});
										}else{
											double transitionScore=explanation.eosTransitionWeights[stateId];
											mCell.setValue(df.format(transitionScore));
											graph.removeCells(new Object[]{cell});
											Object parent = graph.getDefaultParent();
											graph.insertEdge(parent, null, df.format(transitionScore), source, target);
										}
									}
								}else{
									int idx=sId.indexOf(",");
									int itemId=Integer.valueOf(sId.substring(1,idx));
									int stateId=Integer.valueOf(sId.substring(idx+1));
									
									int idx2=tId.indexOf(",");
									int itemId2=Integer.valueOf(tId.substring(1,idx2));
									int stateId2=Integer.valueOf(tId.substring(idx2+1));
									
									if(itemId+1!=itemId2){
										s="Error Edge";
										graph.removeCells(new Object[]{cell});
									}else{
										double transitionScore=explanation.transitionWeights[stateId*explanation.details[0].length+stateId2];
										mCell.setValue(df.format(transitionScore));
										graph.removeCells(new Object[]{cell});
										Object parent = graph.getDefaultParent();
										graph.insertEdge(parent, null, df.format(transitionScore), source, target);
									}
								}
							}
						}else if(mCell.isVertex()){
							 if(id.equals("vStart")||id.equals("vEnd")){
								try{
									txtResult.setText("");
								}catch(Exception ex){
									ex.printStackTrace();
								}
							 }else{
								int idx=id.indexOf(",");
								int i=Integer.valueOf(id.substring(1,idx));
								int j=Integer.valueOf(id.substring(idx+1));
								FeatureWeightScore fws=explanation.details[i][j];
								StringBuilder sb=new StringBuilder();
								sb.append("total score: "+fws.score+"\n\n");
								for(int k=0;k<fws.features.size();k++){
									String feature=fws.features.get(k);
									double weight=fws.weights.get(k);
									sb.append(feature+"  "+df.format(weight)+"\n");
								}
								try{
									txtResult.setText(sb.toString());
								}catch(Exception ex){
									ex.printStackTrace();
								}
							 }
						}
						
					}
					
					graph.repaint();
				}
			}
		});
		
		
		this.getContentPane().add(cmdFrame,BorderLayout.NORTH);
		this.getContentPane().add(graphComponent,BorderLayout.CENTER);
		this.getContentPane().add(resultFrame,BorderLayout.SOUTH);
		
		this.clickButton();
	}
	
	private int nodeWidth=60;
	private int nodeHeight=25;
	private int nodeXInterval=20;
	private int nodeYInterval=40; 
	
	private static final int WIDTH=800;
	private static final int HEIGHT=600;

	private String nodeStyle="ROUNDED;fillColor=white;fontColor=blue";
	private String edgeStyle="ROUNDED;fillColor=white;fontColor=blue";
	private DecimalFormat df=new DecimalFormat("##.0");
	private void draw(){
		graph.selectAll();
		graph.removeCells();
		if(this.explanation==null) return;
		int charNum=explanation.details.length;
		int labelNum=explanation.details[0].length;
		int totalWidth=charNum*nodeWidth+(charNum-1)*nodeXInterval+nodeWidth*2+nodeXInterval*2;
		int totalHeight=labelNum*nodeHeight+(labelNum-1)*nodeYInterval;
		
		int xStart=(WIDTH-totalWidth)/2;
		xStart=Math.max(0, xStart);
		int yStart=30;
		//yStart=Math.max(0, yStart);
		
		
		Object parent = graph.getDefaultParent();

		graph.getModel().beginUpdate();
		try
		{
//		   Object v1 = graph.insertVertex(parent, null, "Hello", 20, 20, 80,
//		         30);
//		   Object v2 = graph.insertVertex(parent, null, "World!",
//		         240, 150, 80, 30);
//		   graph.insertEdge(parent, null, "Edge", v1, v2);

			Object v1 = graph.insertVertex(parent, "vStart", "Start", xStart, yStart+totalHeight/2, nodeWidth, nodeHeight,nodeStyle);
			startCell=(mxCell) v1;
			int xoffset=xStart+nodeXInterval+nodeWidth+2*nodeWidth;
			Object lastNode=v1;
			String lastLabel=null;
			int lastId=0;
			for(int i=0;i<charNum;i++){
				String curToken=explanation.tokens.get(i);
				int bestTag=explanation.bestTagIds[i];
				int yoffset=yStart;
				
				for(int j=0;j<labelNum;j++){
					String label=explanation.labelTexts[j];
					String scoreStr=df.format(explanation.details[i][j].score);
					Object v2=graph.insertVertex(parent, "v"+i+","+j, curToken+"/"+label+"("+scoreStr+")", xoffset, yoffset, nodeWidth, nodeHeight,nodeStyle);
					yoffset+=nodeYInterval+nodeHeight;
					if(j==bestTag){
						String s="";
						if(i==0){
							s=df.format(explanation.bosTransitionWeights[j]);
						}else{
							s=df.format(explanation.transitionWeights[lastId*labelNum+j]);
						}
						Object edge=graph.insertEdge(parent, null, s, lastNode, v2);
						lastNode=v2;
						lastLabel=label;
						lastId=j;
					}
				}
				xoffset+=nodeXInterval+nodeWidth;
			}
			xoffset+=nodeWidth*2;
			Object v3=graph.insertVertex(parent, "vEnd", "End", xoffset, yStart+totalHeight/2, nodeWidth, nodeHeight,nodeStyle);
			String s=df.format(explanation.eosTransitionWeights[lastId]);
			graph.insertEdge(parent, null, s, lastNode, v3);
		}
		finally
		{
		   graph.getModel().endUpdate();
		}
		graphComponent = new mxGraphComponent(graph);
		
	}
	
	private int[] getIndex(String s){
		int idx=s.indexOf(",");
		return new int[]{Integer.valueOf(s.substring(1,idx)),Integer.valueOf(s.substring(idx+1))};
	}
	
	private mxCell getNextCell(mxCell curCell){
		if(curCell==null) return null;
		List<mxCell> lst=new ArrayList<mxCell>();
		for(int i=0;i<curCell.getEdgeCount();i++){
			mxCell child=(mxCell) curCell.getEdgeAt(i);
			if(child.getSource()!=curCell) continue;
			lst.add(child);
		}
		if(lst.size()==1) return (mxCell) lst.get(0).getTarget();
		return null;
	}
	
	private void calcCurrentPath(){
		if(this.explanation==null) return;
		int itemNum=this.explanation.details.length;
		int labelNum=this.explanation.labelTexts.length;
		if(this.startCell==null) return;
		mxCell curCell=this.startCell;
		double score=0;
		int[] lastIndex=null;
		while(true){			
			mxCell child=this.getNextCell(curCell);
			if(child==null){
				try{
					txtResult.setText("错误的路径！");
				}catch(Exception e){
					
				}
				return;
			}
			String childId=child.getId();
			curCell=child;
			if(lastIndex==null){
				int[] curIndex=this.getIndex(childId);
				if(curIndex[0]!=0){
					try{
						txtResult.setText("错误的路径！");
					}catch(Exception e){
						
					}
					return;
				}
				score+=(explanation.bosTransitionWeights[curIndex[1]]);
				score+=(explanation.details[0][curIndex[1]].score);
				lastIndex=curIndex;
				continue;
			}
			
			if(childId.equals("vEnd")){
				double transitionScore=explanation.eosTransitionWeights[lastIndex[1]];
				score+=transitionScore;
				break;
			}else{
				int[] curIndex=this.getIndex(childId);
				if(lastIndex[0]+1!=curIndex[0]){
					txtResult.setText("错误的路径！");
					return;
				}
				score+=(explanation.transitionWeights[lastIndex[1]*labelNum+curIndex[1]]);
				score+=(explanation.details[curIndex[0]][curIndex[1]].score);
				lastIndex=curIndex;
			}
			
		}
		
		txtResult.setText("当前路径得分： "+score);
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if(args.length!=1){
			System.out.println("Usage: CRFExplainer <model_path>");
			System.exit(-1);
		}
		CRFExplainer frame = new CRFExplainer(args[0]);
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setSize(WIDTH, HEIGHT);
		frame.pack();
		frame.setVisible(true);
		
	}

}
