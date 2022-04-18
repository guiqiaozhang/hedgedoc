/*
 * SPDX-FileCopyrightText: 2021 The HedgeDoc developers (see AUTHORS file)
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */
import {
  OnGatewayConnection,
  OnGatewayDisconnect,
  SubscribeMessage,
  WebSocketGateway,
} from '@nestjs/websockets';
import { parse as parseCookie } from 'cookie';
import { IncomingMessage } from 'http';
import { decoding, encoding } from 'lib0';
import WebSocket from 'ws';
import { applyAwarenessUpdate } from 'y-protocols/awareness';
import { readSyncMessage } from 'y-protocols/sync';

import { ConsoleLoggerService } from '../../logger/console-logger.service';
import { NotesService } from '../../notes/notes.service';
import { PermissionsService } from '../../permissions/permissions.service';
import { SessionService } from '../../session/session.service';
import { UsersService } from '../../users/users.service';
import { HEDGEDOC_SESSION } from '../../utils/session';
import { MessageType } from './message-type';
import { MultiClientAwarenessYDoc } from './multi-client-awareness-y-doc';
import { getNoteFromRealtimePath } from './utils/get-note-from-realtime-path';
import { MessageHandlerCallbackResponse } from './yjs-websocket.adapter';

/**
 * Gateway implementing the realtime logic required for realtime note editing.
 */
@WebSocketGateway({ path: '/realtime/' })
export class RealtimeEditorGateway
  implements OnGatewayConnection, OnGatewayDisconnect
{
  private connectionToNoteId = new Map<WebSocket, string>();
  private connectionToYDoc = new Map<WebSocket, MultiClientAwarenessYDoc>();

  constructor(
    private readonly logger: ConsoleLoggerService,
    private noteService: NotesService,
    private userService: UsersService,
    private permissionsService: PermissionsService,
    private sessionService: SessionService,
  ) {
    this.logger.setContext(RealtimeEditorGateway.name);
  }

  /** Mapping instance keeping track of note-ids and their associated y-doc. */
  private noteYDocMap = new Map<string, MultiClientAwarenessYDoc>();

  /**
   * Handler that is called when a WebSocket client disconnects.
   * Removes the client from their Y.Doc, if they were part of any.
   * @param client The WebSocket client that disconnects.
   */
  handleDisconnect(client: WebSocket): void {
    const yDoc = this.connectionToYDoc.get(client);
    const noteId = this.connectionToNoteId.get(client);
    if (!yDoc) {
      this.logger.log('Undefined y-doc for connection');
      return;
    }
    if (!noteId) {
      this.logger.error('Undefined noteId for connection');
      return;
    }
    yDoc.disconnect(client);
    this.connectionToYDoc.delete(client);
    this.connectionToNoteId.delete(client);
    this.logger.debug(`Client disconnected from note '${noteId}'`);
    if (yDoc.countClients() === 0) {
      // TODO Clean-up Y-Doc and store to database
      this.noteYDocMap.delete(noteId);
      yDoc.destroy();
    }
  }

  handleConnectionError(client: WebSocket, message: string): void {
    // TODO Send error message to client to avoid reconnects for same note
    this.logger.log('Realtime connection closed: ' + message);
    client.close();
  }

  /**
   * Handler that is called for each new WebSocket client connection.
   * Checks whether the requested URL path is valid, whether the requested note
   * exists and whether the requesting user has access to the note.
   * Closes the connection to the client if one of the conditions does not apply.
   *
   * @param client The WebSocket client object.
   * @param req The underlying HTTP request of the WebSocket connection.
   */
  async handleConnection(
    client: WebSocket,
    req: IncomingMessage,
  ): Promise<void> {
    client.on('close', () => this.handleDisconnect(client));

    const cookieHeader = req.headers.cookie;
    if (!cookieHeader) {
      return this.handleConnectionError(client, 'No cookie header present');
    }
    const sessionCookie = parseCookie(cookieHeader);
    const cookieContent = sessionCookie[HEDGEDOC_SESSION];

    // const unsignedCookieContent = unsign();
    // TODO Verify signature of cookie content
    if (!cookieContent) {
      return this.handleConnectionError(
        client,
        `No ${HEDGEDOC_SESSION} cookie found`,
      );
    }
    const sessionId = cookieContent.slice(2).split('.')[0];
    try {
      const username = await this.sessionService.getUsernameFromSessionId(
        sessionId,
      );

      const user = await this.userService.getUserByUsername(username);
      const note = await getNoteFromRealtimePath(
        this.noteService,
        req.url ?? '',
      );

      if (!(await this.permissionsService.mayRead(user, note))) {
        return this.handleConnectionError(
          client,
          `Reading note '${note.id}' by user '${user.username}' denied!`,
        );
      }

      const yDoc = this.getOrCreateYDoc(note.id);
      this.connectionToNoteId.set(client, note.id);
      this.connectionToYDoc.set(client, yDoc);
      yDoc.connect(client);
      this.logger.debug(
        `Connection to note '${note.id}' (${note.publicId}) by user '${user.username}'`,
      );
    } catch (error: unknown) {
      // TODO Send error message to client to avoid reconnects for same note
      return this.handleConnectionError(client, String(error));
    }
  }

  private getOrCreateYDoc(noteId: string): MultiClientAwarenessYDoc {
    const yDoc = this.noteYDocMap.get(noteId);
    if (!yDoc) {
      const yDoc = new MultiClientAwarenessYDoc();
      this.noteYDocMap.set(noteId, yDoc);
      return yDoc;
    } else {
      return yDoc;
    }
  }

  /**
   * Handler that is called when a SYNC message is received from a WebSocket client.
   * SYNC messages are part of the Y-js protocol, containing changes on the note.
   * @param client The WebSocket client that sent the message.
   * @param decoder The decoder instance for decoding the message payload.
   * @returns void If no response should be send for this request back to the client.
   * @returns Uint8Array Binary data that should be send as a response to the message back to the client.
   */
  @SubscribeMessage(MessageType.SYNC)
  handleMessageSync(
    client: WebSocket,
    decoder: decoding.Decoder,
  ): MessageHandlerCallbackResponse {
    this.logger.debug('Received SYNC message');

    const yDoc = this.connectionToYDoc.get(client);
    if (!yDoc) {
      // We didn't get a yDoc
      return Promise.reject();
    }
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, MessageType.SYNC);
    readSyncMessage(decoder, encoder, yDoc, null);
    return Promise.resolve(encoding.toUint8Array(encoder));
  }

  /**
   * Handler that is called when a AWARENESS message is received from a WebSocket client.
   * AWARENESS messages are part of the Y-js protocol, containing e.g. the cursor states.
   * @param client The WebSocket client that sent the message.
   * @param decoder The decoder instance for decoding the message payload.
   * @returns void If no response should be send for this request back to the client.
   * @returns Uint8Array Binary data that should be send as a response to the message back to the client.
   */
  @SubscribeMessage(MessageType.AWARENESS)
  handleMessageAwareness(
    client: WebSocket,
    decoder: decoding.Decoder,
  ): MessageHandlerCallbackResponse {
    this.logger.debug('Received AWARENESS message');

    const yDoc = this.connectionToYDoc.get(client);
    if (!yDoc) {
      // We didn't get a yDoc
      return Promise.reject();
    }
    applyAwarenessUpdate(
      yDoc.awareness,
      decoding.readVarUint8Array(decoder),
      client,
    );
    return Promise.resolve();
  }

  /**
   * Handler that is called when a HEDGEDOC message is received from a WebSocket client.
   * HEDGEDOC messages are custom messages containing other real-time important information like permission changes.
   * @param client The WebSocket client that sent the message.
   * @param decoder The decoder instance for decoding the message payload.
   * @returns void If no response should be send for this request back to the client.
   * @returns Uint8Array Binary data that should be send as a response to the message back to the client.
   */
  @SubscribeMessage(MessageType.HEDGEDOC)
  handleMessageHedgeDoc(
    client: WebSocket,
    decoder: decoding.Decoder,
  ): MessageHandlerCallbackResponse {
    this.logger.debug('Received HEDGEDOC message');
    return Promise.resolve();
  }
}
