import {
  SolidityJsonInput,
  VyperJsonInput,
  FeJsonInput,
  VerificationExport,
  CompilationTarget,
  Metadata,
} from "@ethereum-sourcify/lib-sourcify";
import { ConflictError, NotFoundError } from "../../common/errors";
import { asyncLocalStorage } from "../../common/async-context";
import { VerificationJobId } from "../../routes/types";
import Piscina from "piscina";
import path from "path";
import { filename as verificationWorkerFilename } from "../workers/verificationWorker";
import { v4 as uuidv4 } from "uuid";
import os from "os";
import {
  VerifyError,
  VerifyErrorExport,
  VerifyFromConfluxscanInput,
  type VerifyFromJsonInput,
  VerifyFromMetadataInput,
  type VerifyOutput,
} from "../workers/workerTypes";
import { ConfluxscanResult } from "../utils/confluxscan-util";
import { StoreService } from "../store/StoreService";
import { ChainMap } from "../../server";
import { ChainInstance } from "../../config/Loader";
import {
  findSolcPlatform,
  getSolcExecutable,
  getSolcJs,
} from "@ethereum-sourcify/compilers";
import { keccak256 } from "ethers";
import { matchBytesIgnoreCase, validABIEncoded } from "../utils/util";
import logger from "../log/logger";
import { getCreatorTx } from "../utils/contract-creation-util";

export interface VerificationOptions {
  chains: ChainMap;
  solcRepoPath: string;
  solJsonRepoPath: string;
  vyperRepoPath: string;
  feRepoPath: string;
  initCompilers?: boolean;
  workerIdleTimeout?: number;
  concurrentVerificationsPerWorker?: number;
}

export class VerificationService {
  private readonly initCompilers: boolean | undefined;
  private solcRepoPath: string;
  private solJsonRepoPath: string;
  private chains: ChainMap;
  private store: StoreService;
  private workerPool: Piscina;
  private runningTasks: Set<Promise<void>> = new Set();
  public runningTaskIds: Set<string> = new Set();

  constructor(options: VerificationOptions, store: StoreService) {
    this.initCompilers = options.initCompilers;
    this.solcRepoPath = options.solcRepoPath;
    this.solJsonRepoPath = options.solJsonRepoPath;
    this.chains = options.chains;
    this.store = store;

    const chains = Object.entries(options.chains).reduce(
      (acc, [chainId, chain]) => {
        acc[chainId] = chain.getSourcifyChainObj();
        return acc;
      },
      {} as Record<string, ChainInstance>,
    );

    this.workerPool = new Piscina({
      filename: path.resolve(__dirname, "../workers/workerWrapper.js"),
      workerData: {
        fullpath: verificationWorkerFilename,
        solcRepoPath: options.solcRepoPath,
        solJsonRepoPath: options.solJsonRepoPath,
        vyperRepoPath: options.vyperRepoPath,
        feRepoPath: options.feRepoPath,
        chains,
      },
      minThreads: os.availableParallelism() * 0.5,
      maxThreads: os.availableParallelism() * 1.5,
      idleTimeout: options.workerIdleTimeout || 30000,
      concurrentTasksPerWorker: options.concurrentVerificationsPerWorker || 5,
    });
  }

  public async init() {
    const HOST_SOLC_REPO = "https://binaries.soliditylang.org/";

    if (this.initCompilers) {
      const platform = findSolcPlatform() || "bin"; // fallback to emscripten binaries "bin"
      logger.info(`Initializing compilers for platform ${platform}`);

      // solc binary and solc-js downloads are handled with different helpers
      const downLoadFunc =
        platform === "bin"
          ? (version: string) => getSolcJs(this.solJsonRepoPath, version)
          : // eslint-disable-next-line indent
            (version: string) =>
              getSolcExecutable(this.solcRepoPath, platform, version);

      // get the list of compiler versions
      let solcList: string[];
      try {
        solcList = await fetch(`${HOST_SOLC_REPO}${platform}/list.json`)
          .then((response) => response.json())
          .then((data) =>
            (Object.values(data.releases) as string[])
              .map((str) => str.split("-v")[1]) // e.g. soljson-v0.8.26+commit.8a97fa7a.js or solc-linux-amd64-v0.8.26+commit.8a97fa7a
              .map(
                (str) => (str.endsWith(".js") ? str.slice(0, -3) : str), // remove .js extension
              ),
          );
      } catch (e) {
        throw new Error(`Failed to fetch list of solc versions: ${e}`);
      }

      const chunkSize = 10; // Download in chunks to not overload the Solidity server all at once
      for (let i = 0; i < solcList.length; i += chunkSize) {
        const chunk = solcList.slice(i, i + chunkSize);
        const promises = chunk.map((solcVer) => {
          const now = Date.now();
          return downLoadFunc(solcVer).then(() => {
            logger.debug(
              `Downloaded (or found existing) compiler ${solcVer} in ${Date.now() - now}ms`,
            );
          });
        });

        await Promise.all(promises);
        logger.debug(
          `Batch ${i / chunkSize + 1} - Downloaded ${promises.length} - Total ${i + chunkSize}/${solcList.length}`,
        );
      }

      logger.info("Initialized compilers");
    }
    return true;
  }

  public async close() {
    // Immediately abort all workers. Tasks that still run will have their Promises rejected.
    await this.workerPool.destroy();
    // Here, we wait for the rejected tasks which also waits for writing the failed status to the database.
    await Promise.all(this.runningTasks);
  }

  public async verifyFromJsonInputViaWorker(
    verificationEndpoint: string,
    chainId: number,
    address: string,
    jsonInput: SolidityJsonInput | VyperJsonInput | FeJsonInput,
    compilerVersion: string,
    compilationTarget: CompilationTarget,
    creationTransactionHash?: string,
    constructorArguments?: string,
    licenseType?: number,
    contractLabel?: string,
  ): Promise<VerificationJobId> {
    const verificationId = await this.store.storeVerificationJob(
      new Date(),
      chainId,
      address,
      verificationEndpoint,
    );

    const input: VerifyFromJsonInput = {
      chainId,
      address,
      jsonInput,
      compilerVersion,
      compilationTarget,
      creationTransactionHash,
      constructorArguments,
      licenseType,
      contractLabel,
      traceId: asyncLocalStorage.getStore()?.traceId,
    };

    this.runInBackground(
      verificationId,
      this.verifyViaWorker(verificationId, "verifyFromJsonInput", input)
    );

    return verificationId;
  }

  public async verifyFromMetadataViaWorker(
    verificationEndpoint: string,
    chainId: number,
    address: string,
    metadata: Metadata,
    sources: Record<string, string>,
    creationTransactionHash?: string,
  ): Promise<VerificationJobId> {
    const verificationId = await this.store.storeVerificationJob(
      new Date(),
      chainId,
      address,
      verificationEndpoint,
    );

    const input: VerifyFromMetadataInput = {
      chainId,
      address,
      metadata,
      sources,
      creationTransactionHash,
      traceId: asyncLocalStorage.getStore()?.traceId,
    };

    this.runInBackground(
      verificationId,
      this.verifyViaWorker(verificationId, "verifyFromMetadata", input)
    );

    return verificationId;
  }

  public async verifyFromConfluxscanViaWorker(
    verificationEndpoint: string,
    chainId: number,
    address: string,
    confluxscanResult: ConfluxscanResult
  ): Promise<VerificationJobId> {
    const verificationId = await this.store.storeVerificationJob(
      new Date(),
      chainId,
      address,
      verificationEndpoint
    );

    const input: VerifyFromConfluxscanInput = {
      chainId,
      address,
      confluxscanResult,
      traceId: asyncLocalStorage.getStore()?.traceId
    };

    this.runInBackground(
      verificationId,
      this.verifyViaWorker(verificationId, "verifyFromConfluxscan", input)
    );

    return verificationId;
  }

  public async verifyFromCrossChainViaWorker(
    verificationEndpoint: string,
    chainId: number,
    address: string,
    linkChainIds?: number[],
    creationTransactionHash?: string
  ): Promise<VerificationJobId> {
    const verificationId = await this.store.storeVerificationJob(
      new Date(),
      chainId,
      address,
      verificationEndpoint,
    );

    const chain = this.chains[chainId];
    const bytecode = await chain.getBytecode(address);

    if (bytecode === "0x") {
      await this.store.setJobError(verificationId, new Date(), {
        customCode: "contract_not_deployed",
        errorId: uuidv4(),
      });
      return verificationId;
    }

    const foundCreationTxHash =
      creationTransactionHash ||
      (await getCreatorTx(chain, address)) ||
      undefined;

    const { creationBytecode } = foundCreationTxHash ?
      await chain.getContractCreationBytecodeAndReceipt(address, foundCreationTxHash) :
      {};

    await this.store
      .insertNewSimilarContract(
        chainId,
        address,
        keccak256(bytecode),
        linkChainIds,
        creationBytecode,
        {
          verificationId,
          finishTime: new Date(),
        },
      )
      .catch((error) => {
        let errorExport: VerifyErrorExport;
        if (error instanceof NotFoundError) {
          errorExport = {
            customCode: "no_similar_match_found",
            errorId: uuidv4(),
          };
        } else {
          errorExport = {
            customCode: "internal_error",
            errorId: uuidv4(),
          };
          this.logInternalErrorPath("verify_from_crosschain_insert_similar_contract", {
            verificationId,
            chainId,
            address,
            linkChainIds,
            hasCreationBytecode: !!creationBytecode,
            foundCreationTxHash,
            error,
            errorId: errorExport.errorId,
          });
        }
        return this.store.setJobError(verificationId, new Date(), errorExport);
      });

    return verificationId;
  }

  public isRunning(verificationId: string): boolean {
    return this.runningTaskIds.has(verificationId);
  }

  private verifyViaWorker(
    verificationId: VerificationJobId,
    functionName: string,
    input:
      | VerifyFromJsonInput
      | VerifyFromMetadataInput
      | VerifyFromConfluxscanInput,
  ): Promise<void> {
    const { constructorArguments, licenseType, contractLabel } = input as any;
    return this.workerPool
      .run(input, { name: functionName })
      .then((output: VerifyOutput) => {
        if (output.verificationExport) {
          if (constructorArguments) {
            if (!validABIEncoded(constructorArguments)) {
              throw new VerifyError({
                customCode: "constructor_args_not_abi_encoded",
                errorId: uuidv4()
              });
            }

            const expectValue =
              output.verificationExport.transformations?.creation.values
                .constructorArguments || "";
            logger.info("Check constructor arguments", {
              address: output.verificationExport.address,
              chainId: output.verificationExport.chainId,
              constructorArguments,
              expectValue
            });

            if (!matchBytesIgnoreCase(constructorArguments, expectValue)) {
              throw new VerifyError({
                customCode: "constructor_args_not_match",
                errorId: uuidv4()
              });
            }
          }
          return output.verificationExport;
        } else if (output.errorExport) {
          throw new VerifyError(output.errorExport);
        }
        const errorMessage = `The worker did not return a verification export nor an error export. This should never happen.`;
        throw new Error(errorMessage);
      })
      .then((verification: VerificationExport) => {
        return this.store.storeVerification(
          verification,
          {
            verificationId,
            finishTime: new Date()
          },
          licenseType,
          contractLabel
        );
      })
      .catch((error) => {
        let errorExport: VerifyErrorExport;
        if (error instanceof VerifyError) {
          // error comes from the verification worker
          logger.debug("Received verification error from worker", {
            verificationId,
            errorExport: {
              ...error.errorExport,
              // Don't log the full bytecodes because it's too long
              onchainRuntimeCode: error.errorExport?.onchainRuntimeCode
                ? error.errorExport.onchainRuntimeCode.slice(0, 200) + "..."
                : error.errorExport?.onchainRuntimeCode,
              recompiledRuntimeCode: error.errorExport?.recompiledRuntimeCode
                ? error.errorExport.recompiledRuntimeCode.slice(0, 200) + "..."
                : error.errorExport?.recompiledRuntimeCode,
              onchainCreationCode: error.errorExport?.onchainCreationCode
                ? error.errorExport.onchainCreationCode.slice(0, 200) + "..."
                : error.errorExport?.onchainCreationCode,
              recompiledCreationCode: error.errorExport?.recompiledCreationCode
                ? error.errorExport.recompiledCreationCode.slice(0, 200) + "..."
                : error.errorExport?.recompiledCreationCode
            }
          });
          errorExport = error.errorExport;
        } else if (error instanceof ConflictError) {
          // returned by StorageService if match already exists and new one is not better
          errorExport = {
            customCode: "already_verified",
            errorId: uuidv4()
          };
        } else {
          errorExport = {
            customCode: "internal_error",
            errorId: uuidv4()
          };
          this.logInternalErrorPath("verify_via_worker_unexpected_error", {
            verificationId,
            functionName,
            inputChainId: (input as any)?.chainId,
            inputAddress: (input as any)?.address,
            inputTraceId: (input as any)?.traceId,
            error,
            errorId: errorExport.errorId
          });
        }

        return this.store.setJobError(verificationId, new Date(), errorExport);
      });
  }

  private logInternalErrorPath(path: string, context: Record<string, unknown>) {
    logger.error("Verification internal_error path triggered", {
      internalErrorPath: path,
      ...context,
    });
  }

  private runInBackground(verificationId: string, promise: Promise<void>): void {
    const task = promise.finally(() => {
      this.runningTaskIds.delete(verificationId);
      this.runningTasks.delete(task);
    });
    this.runningTaskIds.add(verificationId);
    this.runningTasks.add(task);
  }
}
