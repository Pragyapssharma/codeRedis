import java.util.ArrayList;

public class Pattern {
	
	public static ArrayList<ArrayList<Character>> interestingPattern(int N) {
		ArrayList<ArrayList<Character>> listOfList = new ArrayList<>();
		
        char[] ch = {'A','B','C','D','E'};
        int a = 0;
        int b = 0;
		for(int i=N; i>=1;i--){
			ArrayList<Character> list = new ArrayList<>();
            list.add(ch[i-1]);
            a = a+1;
            b= i;
            for(int j=1; j<=N; j++){
                if(j<a) {
                    list.add(ch[b]);
                    b = b+1;
                }
                else 
                    list.add(' ');
            }
            listOfList.add(list);
        }
        return listOfList;
	}

	
	public static void main(String[] args) {
		
		Integer[] in = {4,5,4,3,2};
		
		for (int n : in) {
            ArrayList<ArrayList<Character>> result = interestingPattern(n);
            
            for (ArrayList<Character> row : result) {
                for (char ch : row) {
                    System.out.print(ch);
                }
                System.out.println();
            }
            System.out.println();
        }


		
	}

}
