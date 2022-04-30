from dataGatherer import dataGatherer
from dataRefiner import serverPipeline

def main():
    dataGatherer()
    
    serverPipeline()

main()